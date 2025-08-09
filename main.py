"""
Real-Time Streaming Transcription Service with AssemblyAI Universal-Streaming (v3)
Updated to use the new Universal-Streaming API
"""

import asyncio
import websockets
import json
import base64
import io
import tempfile
import logging
import time
import uuid
import os
import shutil
from typing import AsyncGenerator, Optional, Dict, Any, List
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import aiofiles

from fastapi import FastAPI, WebSocket, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, HttpUrl

import yt_dlp
from pydub import AudioSegment
import soundfile as sf
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

@dataclass
class TranscriptChunk:
    text: str
    confidence: float
    turn_order: int
    end_of_turn: bool
    is_formatted: bool
    chunk_id: int

@dataclass
class StreamProgress:
    chunk_id: int
    timestamp: float
    duration_processed: float
    total_duration: Optional[float]
    status: str

class StreamingRequest(BaseModel):
    url: HttpUrl
    chunk_duration_ms: Optional[int] = 200  # Optimal for AssemblyAI
    enable_formatting: Optional[bool] = True
    sample_rate: Optional[int] = 16000

class UniversalStreamingPipeline:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.websocket: Optional[websockets.WebSocketServerProtocol] = None
        self.session_active = False
        self.transcript_chunks: List[TranscriptChunk] = []
        self.executor = ThreadPoolExecutor(max_workers=2)
        self._message_queue = asyncio.Queue()
        
    async def connect_websocket(
        self, 
        sample_rate: int = 16000,
        enable_formatting: bool = True
    ) -> None:
        """Connect to AssemblyAI Universal-Streaming API"""
        try:
            # Universal-Streaming v3 WebSocket URL
            uri = "wss://streaming.assemblyai.com/v3/ws"
            
            # Use Authorization header with API key (no Bearer prefix)
            headers = {"Authorization": self.api_key}
            
            self.websocket = await websockets.connect(uri, additional_headers=headers)
            self.session_active = True
            
            logger.info("Connected to AssemblyAI Universal-Streaming (v3)")
            
            # Wait for Begin message
            begin_msg = await self.websocket.recv()
            begin_data = json.loads(begin_msg)
            
            if begin_data.get("type") == "Begin":
                session_id = begin_data.get("id")
                expires_at = begin_data.get("expires_at")
                logger.info(f"Session started: {session_id}, expires at: {expires_at}")
                
                await self._message_queue.put({
                    "type": "session_started",
                    "data": f"Universal-Streaming session {session_id} started"
                })
            else:
                logger.warning(f"Unexpected begin message: {begin_data}")
            
            # Start listening for transcription results (Turn messages)
            asyncio.create_task(self._listen_for_transcripts())
            
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            raise

    async def _listen_for_transcripts(self) -> None:
        """Listen for Turn messages from Universal-Streaming"""
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    message_type = data.get("type")
                    
                    if message_type == "Turn":
                        # Handle Universal-Streaming Turn message
                        transcript_text = data.get("transcript", "").strip()
                        end_of_turn = data.get("end_of_turn", False)
                        turn_order = data.get("turn_order", 0)
                        turn_is_formatted = data.get("turn_is_formatted", False)
                        end_of_turn_confidence = data.get("end_of_turn_confidence", 1.0)
                        
                        if transcript_text:  # Only process non-empty transcripts
                            transcript_chunk = TranscriptChunk(
                                text=transcript_text,
                                confidence=end_of_turn_confidence,
                                turn_order=turn_order,
                                end_of_turn=end_of_turn,
                                is_formatted=turn_is_formatted,
                                chunk_id=turn_order
                            )
                            
                            await self._message_queue.put({
                                "type": "transcript",
                                "data": transcript_chunk
                            })
                            
                            # Store completed turns
                            if end_of_turn:
                                self.transcript_chunks.append(transcript_chunk)
                                
                            logger.info(f"Turn {turn_order}: {transcript_text} (end_of_turn: {end_of_turn})")
                        
                    elif message_type == "Termination":
                        logger.info("Universal-Streaming session terminated")
                        await self._message_queue.put({
                            "type": "session_ended",
                            "data": "Session terminated"
                        })
                        break
                        
                    else:
                        logger.debug(f"Unknown message type: {message_type}")
                        
                except json.JSONDecodeError:
                    logger.warning(f"Received non-JSON message: {message}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed")
        except Exception as e:
            logger.error(f"Error listening for transcripts: {e}")
            await self._message_queue.put({
                "type": "error", 
                "data": f"Transcript listening error: {str(e)}"
            })

    async def _send_audio_chunk(self, audio_chunk: bytes) -> None:
        """Send raw audio bytes to Universal-Streaming WebSocket"""
        if not self.websocket or not self.session_active:
            raise Exception("WebSocket not connected")
        
        try:
            # Universal-Streaming expects raw PCM16 audio bytes, not JSON
            await self.websocket.send(audio_chunk)
            
        except Exception as e:
            logger.error(f"Error sending audio chunk: {e}")
            raise

    async def _end_transcription_session(self) -> None:
        """Close the Universal-Streaming session"""
        if self.websocket and self.session_active:
            try:
                # Universal-Streaming closes when WebSocket closes
                await self.websocket.close()
                self.session_active = False
                logger.info("Universal-Streaming session ended")
            except Exception as e:
                logger.error(f"Error ending session: {e}")

    async def _listen_for_transcripts(self) -> None:
        """Listen for incoming transcription results from Universal-Streaming"""
        try:
            turn_order = 0
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    message_type = data.get("type")
                    
                    if message_type == "session_started":
                        logger.info("Universal-Streaming session started")
                        await self._message_queue.put({
                            "type": "session_started",
                            "data": "Session started successfully"
                        })
                        
                    elif message_type == "transcript":
                        # Handle Universal-Streaming transcript format
                        transcript_text = data.get("transcript", "").strip()
                        end_of_turn = data.get("end_of_turn", False)
                        turn_is_formatted = data.get("turn_is_formatted", False)
                        turn_order_val = data.get("turn_order", turn_order)
                        end_of_turn_confidence = data.get("end_of_turn_confidence", 1.0)
                        
                        if transcript_text:  # Only process non-empty transcripts
                            transcript_chunk = TranscriptChunk(
                                text=transcript_text,
                                confidence=end_of_turn_confidence,
                                turn_order=turn_order_val,
                                end_of_turn=end_of_turn,
                                is_formatted=turn_is_formatted,
                                chunk_id=turn_order_val
                            )
                            
                            await self._message_queue.put({
                                "type": "transcript",
                                "data": transcript_chunk
                            })
                            
                            # Store completed turns
                            if end_of_turn:
                                self.transcript_chunks.append(transcript_chunk)
                                turn_order += 1
                                
                            logger.info(f"Turn {turn_order_val}: {transcript_text} (end_of_turn: {end_of_turn})")
                        
                    elif message_type == "error":
                        error_msg = data.get("message", "Unknown error")
                        logger.error(f"Universal-Streaming error: {error_msg}")
                        await self._message_queue.put({
                            "type": "error",
                            "data": error_msg
                        })
                        
                    elif message_type == "session_ended":
                        logger.info("Universal-Streaming session ended")
                        await self._message_queue.put({
                            "type": "session_ended",
                            "data": "Session ended"
                        })
                        
                except json.JSONDecodeError:
                    logger.warning(f"Received non-JSON message: {message}")
                except Exception as e:
                    logger.error(f"Error processing transcript message: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed")
        except Exception as e:
            logger.error(f"Error listening for transcripts: {e}")
            await self._message_queue.put({
                "type": "error", 
                "data": f"Transcript listening error: {str(e)}"
            })

    async def download_and_process_video(
        self, 
        video_url: str,
        chunk_duration_ms: int = 200
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Download video and process audio in streaming fashion"""
        temp_dir = None
        
        try:
            # Setup temporary directory
            temp_dir = tempfile.mkdtemp(prefix="streaming_")
            
            # Get video info first
            ydl_opts_info = {
                'quiet': True,
                'no_warnings': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                info = ydl.extract_info(video_url, download=False)
                video_title = info.get('title', 'Unknown Video')
                total_duration = info.get('duration', 0)
                
                yield {
                    "type": "video_info",
                    "title": video_title,
                    "duration": total_duration,
                    "status": "starting_download"
                }
                
                logger.info(f"Processing: {video_title} ({total_duration}s)")
            
            # Download with best audio quality
            audio_file = await self._download_audio(video_url, temp_dir)
            
            yield {
                "type": "download_complete",
                "audio_file": os.path.basename(audio_file),
                "status": "starting_audio_processing"
            }
            
            # Process audio in chunks
            async for chunk_result in self._process_audio_chunks(
                audio_file, chunk_duration_ms, total_duration
            ):
                yield chunk_result
                
        except Exception as e:
            logger.error(f"Error in video processing: {e}")
            yield {
                "type": "error",
                "message": str(e)
            }
        finally:
            # Cleanup
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    async def _download_audio(self, video_url: str, temp_dir: str) -> str:
        """Download and extract audio from video"""
        def download_sync():
            ydl_opts = {
                'format': 'bestaudio[ext=m4a]/bestaudio/best',
                'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
                'quiet': True,
                'no_warnings': True,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'wav',
                    'preferredquality': '192',
                }],
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            
            # Find the downloaded file
            for file in os.listdir(temp_dir):
                if file.endswith('.wav'):
                    return os.path.join(temp_dir, file)
            
            raise FileNotFoundError("Audio file not found after download")
        
        # Run download in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        audio_file = await loop.run_in_executor(self.executor, download_sync)
        
        logger.info(f"Audio downloaded: {audio_file}")
        return audio_file

    async def _process_audio_chunks(
        self, 
        audio_file: str, 
        chunk_duration_ms: int,
        total_duration: float
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Process audio file in chunks and send to Universal-Streaming"""
        
        def load_and_convert_audio():
            # Load audio using soundfile (better Python 3.13 compatibility)
            try:
                audio_data, sample_rate = sf.read(audio_file)
                
                # Convert to mono if stereo
                if len(audio_data.shape) > 1:
                    audio_data = np.mean(audio_data, axis=1)
                
                # Resample to 16kHz if needed
                if sample_rate != 16000:
                    from scipy import signal
                    num_samples = int(len(audio_data) * 16000 / sample_rate)
                    audio_data = signal.resample(audio_data, num_samples)
                    sample_rate = 16000
                
                # Convert to AudioSegment for chunking
                # Scale to 16-bit integer range
                audio_data_int = (audio_data * 32767).astype(np.int16)
                audio_segment = AudioSegment(
                    audio_data_int.tobytes(),
                    frame_rate=16000,
                    sample_width=2,
                    channels=1
                )
                
                return audio_segment
            except Exception as e:
                logger.error(f"Error loading audio with soundfile: {e}")
                # Fallback to pydub
                audio = AudioSegment.from_file(audio_file)
                return audio.set_frame_rate(16000).set_channels(1).set_sample_width(2)
        
        # Load audio in thread pool
        loop = asyncio.get_event_loop()
        audio = await loop.run_in_executor(self.executor, load_and_convert_audio)
        
        total_chunks = len(audio) // chunk_duration_ms
        logger.info(f"Processing {total_chunks} audio chunks of {chunk_duration_ms}ms each")
        
        # Process chunks
        for i in range(0, len(audio), chunk_duration_ms):
            chunk = audio[i:i + chunk_duration_ms]
            
            if len(chunk) < 50:  # Skip very short chunks
                continue
            
            # Convert chunk to bytes (PCM16)
            chunk_bytes = chunk.raw_data
            
            # Send to Universal-Streaming
            await self._send_audio_chunk(chunk_bytes)
            
            # Calculate progress
            timestamp = i / 1000.0
            duration_processed = min(i + chunk_duration_ms, len(audio)) / 1000.0
            
            progress = StreamProgress(
                chunk_id=i // chunk_duration_ms,
                timestamp=timestamp,
                duration_processed=duration_processed,
                total_duration=total_duration,
                status="processing"
            )
            
            yield {
                "type": "progress",
                "chunk_id": progress.chunk_id,
                "timestamp": progress.timestamp,
                "duration_processed": progress.duration_processed,
                "total_duration": progress.total_duration,
                "progress_percent": (duration_processed / total_duration * 100) if total_duration > 0 else 0
            }
            
            # Small delay to prevent overwhelming the API
            await asyncio.sleep(0.05)
        
        # Signal end of audio stream
        await self._end_transcription_session()
        
        yield {
            "type": "audio_complete",
            "total_chunks_sent": total_chunks,
            "status": "waiting_for_final_transcripts"
        }

    async def _send_audio_chunk(self, audio_chunk: bytes) -> None:
        """Send audio chunk to Universal-Streaming WebSocket"""
        if not self.websocket or not self.session_active:
            raise Exception("WebSocket not connected")
        
        try:
            # Universal-Streaming expects raw audio data, not base64
            # Send as binary message
            await self.websocket.send(audio_chunk)
            
        except Exception as e:
            logger.error(f"Error sending audio chunk: {e}")
            raise

    async def _end_transcription_session(self) -> None:
        """Signal end of transcription session"""
        if self.websocket and self.session_active:
            try:
                end_message = {"type": "session_end"}
                await self.websocket.send(json.dumps(end_message))
                self.session_active = False
                logger.info("Universal-Streaming session ended")
            except Exception as e:
                logger.error(f"Error ending session: {e}")

    async def get_message(self) -> Optional[Dict[str, Any]]:
        """Get next message from the transcript queue"""
        try:
            return await asyncio.wait_for(self._message_queue.get(), timeout=0.1)
        except asyncio.TimeoutError:
            return None

    async def stream_transcription(
        self,
        video_url: str,
        chunk_duration_ms: int = 200,
        enable_formatting: bool = True,
        sample_rate: int = 16000
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Main streaming transcription pipeline using Universal-Streaming"""
        try:
            # Connect to Universal-Streaming WebSocket
            await self.connect_websocket(
                sample_rate=sample_rate,
                enable_formatting=enable_formatting
            )
            
            yield {
                "type": "connection_established",
                "status": "connected_to_universal_streaming"
            }
            
            # Start transcript message streaming
            transcript_task = asyncio.create_task(self._stream_transcript_messages())
            
            # Process video and stream audio chunks
            async for video_result in self.download_and_process_video(video_url, chunk_duration_ms):
                yield video_result
                
                # Check for any transcript messages
                try:
                    while True:
                        message = await asyncio.wait_for(self._message_queue.get(), timeout=0.01)
                        if message:
                            if message["type"] == "transcript":
                                chunk = message["data"]
                                yield {
                                    "type": "transcript",
                                    "text": chunk.text,
                                    "confidence": chunk.confidence,
                                    "end_of_turn": chunk.end_of_turn,
                                    "turn_order": chunk.turn_order,
                                    "is_formatted": chunk.is_formatted
                                }
                            elif message["type"] in ["session_started", "session_ended"]:
                                yield {"type": message["type"], "message": message["data"]}
                            elif message["type"] == "error":
                                yield {"type": "error", "message": message["data"]}
                except asyncio.TimeoutError:
                    pass  # No transcript messages available, continue
            
            # Wait a bit for final transcripts
            await asyncio.sleep(2)
            
            # Get any remaining transcript messages
            final_transcripts = []
            try:
                while True:
                    message = await asyncio.wait_for(self._message_queue.get(), timeout=0.1)
                    if message and message["type"] == "transcript":
                        chunk = message["data"]
                        final_transcripts.append(chunk.text)
                        yield {
                            "type": "transcript",
                            "text": chunk.text,
                            "confidence": chunk.confidence,
                            "end_of_turn": chunk.end_of_turn,
                            "turn_order": chunk.turn_order,
                            "is_formatted": chunk.is_formatted
                        }
            except asyncio.TimeoutError:
                pass
            
            # Final summary
            all_transcript_text = " ".join([chunk.text for chunk in self.transcript_chunks])
            yield {
                "type": "complete",
                "total_transcripts": len(self.transcript_chunks),
                "final_text": all_transcript_text
            }
            
            # Cancel transcript task
            transcript_task.cancel()
            
        except Exception as e:
            logger.error(f"Streaming transcription error: {e}")
            yield {"type": "error", "message": str(e)}
        finally:
            if self.websocket:
                await self.websocket.close()

    async def _stream_transcript_messages(self):
        """Background task to stream transcript messages"""
        while self.session_active:
            try:
                message = await asyncio.wait_for(self._message_queue.get(), timeout=1.0)
                if message:
                    # Messages are handled in the main stream_transcription method
                    # This is just to keep the queue processing
                    await self._message_queue.put(message)  # Put it back
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in transcript message streaming: {e}")
                break


# FastAPI Application
app = FastAPI(title="Universal-Streaming Transcription Service", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://your-domain.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "message": "Universal-Streaming Transcription Service", 
        "version": "3.0.0",
        "api": "AssemblyAI Universal-Streaming (v3)",
        "features": ["streaming_download", "universal_streaming", "websocket_api"]
    }

@app.get("/health")
async def health():
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    return {
        "status": "healthy",
        "assemblyai_configured": bool(api_key and api_key != "your_assemblyai_api_key_here"),
        "streaming_enabled": True,
        "api_version": "v3_universal_streaming"
    }

@app.websocket("/stream-transcribe")
async def websocket_transcribe(websocket: WebSocket):
    """WebSocket endpoint for Universal-Streaming transcription"""
    await websocket.accept()
    
    try:
        # Get parameters
        data = await websocket.receive_json()
        video_url = data.get("video_url")
        
        if not video_url:
            await websocket.send_json({
                "type": "error", 
                "message": "video_url is required"
            })
            return
        
        # Get API key from environment
        api_key = os.getenv('ASSEMBLYAI_API_KEY')
        if not api_key or api_key == "your_assemblyai_api_key_here":
            await websocket.send_json({
                "type": "error", 
                "message": "AssemblyAI API key not configured properly"
            })
            return
        
        # Optional parameters
        chunk_duration = data.get("chunk_duration_ms", 200)
        enable_formatting = data.get("enable_formatting", True)
        sample_rate = data.get("sample_rate", 16000)
        
        # Initialize pipeline
        pipeline = UniversalStreamingPipeline(api_key)
        
        # Stream transcription results
        async for result in pipeline.stream_transcription(
            video_url,
            chunk_duration_ms=chunk_duration,
            enable_formatting=enable_formatting,
            sample_rate=sample_rate
        ):
            await websocket.send_json(result)
            
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.send_json({
            "type": "error", 
            "message": f"Pipeline failed: {str(e)}"
        })
    finally:
        await websocket.close()

@app.post("/stream-transcribe")
async def http_stream_transcribe(request: StreamingRequest):
    """HTTP streaming endpoint for transcription"""
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    if not api_key or api_key == "your_assemblyai_api_key_here":
        raise HTTPException(status_code=500, detail="AssemblyAI API key not configured")
    
    pipeline = UniversalStreamingPipeline(api_key)
    
    async def generate_stream():
        try:
            async for result in pipeline.stream_transcription(
                str(request.url),
                chunk_duration_ms=request.chunk_duration_ms,
                enable_formatting=request.enable_formatting,
                sample_rate=request.sample_rate
            ):
                yield f"data: {json.dumps(result)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        generate_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    # Check for API key
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    if not api_key or api_key == "your_assemblyai_api_key_here":
        print("⚠️  WARNING: ASSEMBLYAI_API_KEY not properly configured in .env file")
        print("Please edit .env and add your API key from https://www.assemblyai.com/app/account")
    else:
        print("✅ AssemblyAI API key configured")
    
    print("🚀 Starting Universal-Streaming Transcription Service")
    print("📡 Using AssemblyAI Universal-Streaming (v3) API")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0", 
        port=8002,
        reload=True,
        log_level="info"
    )