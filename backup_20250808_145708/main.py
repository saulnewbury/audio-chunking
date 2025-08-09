"""
Real-Time Streaming Transcription Service with AssemblyAI
Updated version of your existing chunking service with streaming capabilities
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
    start_time: float
    end_time: float
    is_final: bool
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
    custom_vocabulary: Optional[List[str]] = None

class RealTimeTranscriptionPipeline:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.websocket: Optional[websockets.WebSocketServerProtocol] = None
        self.session_active = False
        self.transcript_chunks: List[TranscriptChunk] = []
        self.executor = ThreadPoolExecutor(max_workers=2)
        self._message_queue = asyncio.Queue()
        
    async def create_temp_token(self) -> str:
        """Create temporary token for WebSocket authentication"""
        try:
            async with aiohttp.ClientSession() as session:
                headers = {"authorization": self.api_key}
                data = {"expires_in": 3600}  # 1 hour
                
                async with session.post(
                    "https://api.assemblyai.com/v2/realtime/token",
                    json=data,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result["token"]
                    else:
                        error_text = await response.text()
                        raise Exception(f"Failed to create temp token: {response.status} - {error_text}")
        except Exception as e:
            logger.error(f"Error creating temp token: {e}")
            raise

    async def connect_websocket(
        self, 
        sample_rate: int = 16000,
        enable_formatting: bool = True,
        custom_vocabulary: Optional[List[str]] = None
    ) -> None:
        """Establish WebSocket connection to AssemblyAI"""
        try:
            # Get temporary token
            temp_token = await self.create_temp_token()
            
            # Build WebSocket URI with parameters
            uri = f"wss://api.assemblyai.com/v2/realtime/ws?sample_rate={sample_rate}&token={temp_token}"
            
            if enable_formatting:
                uri += "&format_text=true"
            
            # Connect to WebSocket
            self.websocket = await websockets.connect(uri)
            self.session_active = True
            
            logger.info("WebSocket connected successfully")
            
            # Send configuration if custom vocabulary provided
            if custom_vocabulary:
                config_message = {
                    "word_boost": custom_vocabulary
                }
                await self.websocket.send(json.dumps(config_message))
                logger.info(f"Sent custom vocabulary: {len(custom_vocabulary)} words")
            
            # Start listening for transcription results
            asyncio.create_task(self._listen_for_transcripts())
            
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            raise

    async def _listen_for_transcripts(self) -> None:
        """Listen for incoming transcription results"""
        try:
            chunk_id = 0
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    
                    # Handle different message types
                    if "text" in data and data["text"].strip():
                        is_final = data.get("message_type") == "FinalTranscript"
                        
                        transcript_chunk = TranscriptChunk(
                            text=data["text"],
                            confidence=data.get("confidence", 1.0),
                            start_time=time.time(),  # Approximate timestamp
                            end_time=time.time(),
                            is_final=is_final,
                            chunk_id=chunk_id
                        )
                        
                        await self._message_queue.put({
                            "type": "transcript",
                            "data": transcript_chunk
                        })
                        
                        if is_final:
                            self.transcript_chunks.append(transcript_chunk)
                            chunk_id += 1
                            
                        logger.info(f"{'Final' if is_final else 'Partial'}: {data['text']}")
                        
                    elif "error" in data:
                        logger.error(f"AssemblyAI error: {data['error']}")
                        await self._message_queue.put({
                            "type": "error",
                            "data": data["error"]
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
        """
        Download video and process audio in streaming fashion
        """
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
        """Process audio file in chunks and send to AssemblyAI"""
        
        def load_and_convert_audio():
            # Load audio
            audio = AudioSegment.from_file(audio_file)
            
            # Convert to optimal format for AssemblyAI
            audio = audio.set_frame_rate(16000).set_channels(1).set_sample_width(2)
            
            return audio
        
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
            
            # Convert chunk to bytes
            chunk_io = io.BytesIO()
            chunk.export(chunk_io, format="wav")
            chunk_bytes = chunk_io.getvalue()
            
            # Send to AssemblyAI
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
        """Send audio chunk to AssemblyAI WebSocket"""
        if not self.websocket or not self.session_active:
            raise Exception("WebSocket not connected")
        
        try:
            # Encode audio data as base64
            audio_data = base64.b64encode(audio_chunk).decode('utf-8')
            
            # Send to AssemblyAI
            message = {"audio_data": audio_data}
            await self.websocket.send(json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error sending audio chunk: {e}")
            raise

    async def _end_transcription_session(self) -> None:
        """Signal end of transcription session"""
        if self.websocket and self.session_active:
            try:
                end_message = {"terminate_session": True}
                await self.websocket.send(json.dumps(end_message))
                self.session_active = False
                logger.info("Transcription session ended")
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
        custom_vocabulary: Optional[List[str]] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Main streaming transcription pipeline
        """
        try:
            # Connect to AssemblyAI WebSocket
            await self.connect_websocket(
                enable_formatting=enable_formatting,
                custom_vocabulary=custom_vocabulary
            )
            
            yield {
                "type": "connection_established",
                "status": "connected_to_assemblyai"
            }
            
            # Start video processing and transcript listening concurrently
            video_task = asyncio.create_task(
                self._stream_video_processing(video_url, chunk_duration_ms)
            )
            
            transcript_task = asyncio.create_task(
                self._stream_transcript_messages()
            )
            
            # Stream results from both tasks
            pending = {video_task, transcript_task}
            
            while pending:
                done, pending = await asyncio.wait(
                    pending, 
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=1.0
                )
                
                for task in done:
                    if task == video_task:
                        try:
                            async for result in task.result():
                                yield result
                        except Exception as e:
                            yield {"type": "error", "message": f"Video processing error: {e}"}
                        finally:
                            # Remove completed video task
                            pending.discard(video_task)
                    
                    elif task == transcript_task:
                        try:
                            async for result in task.result():
                                yield result
                        except Exception as e:
                            yield {"type": "error", "message": f"Transcript error: {e}"}
                        finally:
                            # Remove completed transcript task
                            pending.discard(transcript_task)
            
            # Final summary
            yield {
                "type": "complete",
                "total_transcripts": len(self.transcript_chunks),
                "final_text": " ".join([chunk.text for chunk in self.transcript_chunks])
            }
            
        except Exception as e:
            logger.error(f"Streaming transcription error: {e}")
            yield {"type": "error", "message": str(e)}
        finally:
            if self.websocket:
                await self.websocket.close()

    async def _stream_video_processing(self, video_url: str, chunk_duration_ms: int):
        """Stream video processing results"""
        async for result in self.download_and_process_video(video_url, chunk_duration_ms):
            yield result

    async def _stream_transcript_messages(self):
        """Stream transcript messages"""
        while self.session_active or not self._message_queue.empty():
            message = await self.get_message()
            if message:
                if message["type"] == "transcript":
                    chunk = message["data"]
                    yield {
                        "type": "transcript",
                        "text": chunk.text,
                        "confidence": chunk.confidence,
                        "is_final": chunk.is_final,
                        "chunk_id": chunk.chunk_id
                    }
                elif message["type"] == "error":
                    yield {"type": "error", "message": message["data"]}
            else:
                await asyncio.sleep(0.1)


# FastAPI Application
app = FastAPI(title="Real-Time Streaming Transcription Service", version="3.0.0")

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
        "message": "Real-Time Streaming Transcription Service", 
        "version": "3.0.0",
        "features": ["streaming_download", "real_time_transcription", "websocket_api"]
    }

@app.get("/health")
async def health():
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    return {
        "status": "healthy",
        "assemblyai_configured": bool(api_key),
        "streaming_enabled": True
    }

@app.websocket("/stream-transcribe")
async def websocket_transcribe(websocket: WebSocket):
    """WebSocket endpoint for real-time transcription streaming"""
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
        if not api_key:
            await websocket.send_json({
                "type": "error", 
                "message": "AssemblyAI API key not configured"
            })
            return
        
        # Optional parameters
        chunk_duration = data.get("chunk_duration_ms", 200)
        enable_formatting = data.get("enable_formatting", True)
        custom_vocabulary = data.get("custom_vocabulary", None)
        
        # Initialize pipeline
        pipeline = RealTimeTranscriptionPipeline(api_key)
        
        # Stream transcription results
        async for result in pipeline.stream_transcription(
            video_url,
            chunk_duration_ms=chunk_duration,
            enable_formatting=enable_formatting,
            custom_vocabulary=custom_vocabulary
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
    if not api_key:
        raise HTTPException(status_code=500, detail="AssemblyAI API key not configured")
    
    pipeline = RealTimeTranscriptionPipeline(api_key)
    
    async def generate_stream():
        try:
            async for result in pipeline.stream_transcription(
                str(request.url),
                chunk_duration_ms=request.chunk_duration_ms,
                enable_formatting=request.enable_formatting,
                custom_vocabulary=request.custom_vocabulary
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
    if not os.getenv('ASSEMBLYAI_API_KEY'):
        print("WARNING: ASSEMBLYAI_API_KEY environment variable not set!")
        print("Please set it in your .env file or environment")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0", 
        port=8002,
        reload=True,
        log_level="info"
    )