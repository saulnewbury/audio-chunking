"""
Fast Concurrent Chunked Transcription Service
Processes videos in parallel chunks for maximum speed
Target: 5 seconds for 20-minute video
"""

import asyncio
import json
import os
import tempfile
import logging
import time
import shutil
from typing import AsyncGenerator, Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import aiohttp

from fastapi import FastAPI, WebSocket, HTTPException
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
class TranscriptResult:
    chunk_id: int
    start_time: float
    end_time: float
    text: str
    confidence: float
    processing_time: float

@dataclass
class ProcessingStats:
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    total_duration: float
    processing_time: float
    speedup_factor: float

class FastTranscriptionRequest(BaseModel):
    url: HttpUrl
    chunk_duration_seconds: Optional[float] = 60.0  # Larger 60-second chunks
    max_concurrent_chunks: Optional[int] = 25       # Much higher concurrency
    overlap_seconds: Optional[float] = 3.0          # Slightly more overlap

class FastChunkedTranscriptionService:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = None
        self.executor = ThreadPoolExecutor(max_workers=4)
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def transcribe_video_fast(
        self,
        video_url: str,
        chunk_duration: float = 60.0,  # Larger chunks
        max_concurrent: int = 25,      # Higher concurrency
        overlap_seconds: float = 3.0   # More overlap
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Fast transcription using concurrent chunk processing"""
        
        temp_dir = None
        start_time = time.time()
        
        try:
            # Setup
            temp_dir = tempfile.mkdtemp(prefix="fast_transcription_")
            
            # Download and get video info
            yield {"type": "status", "message": "Downloading video..."}
            
            video_info, audio_file = await self._download_video(video_url, temp_dir)
            
            yield {
                "type": "video_info",
                "title": video_info["title"],
                "duration": video_info["duration"],
                "status": "preparing_chunks"
            }
            
            # Create audio chunks
            yield {"type": "status", "message": "Creating audio chunks..."}
            
            chunks = await self._create_audio_chunks(
                audio_file, 
                chunk_duration, 
                overlap_seconds,
                video_info["duration"]
            )
            
            yield {
                "type": "chunks_created",
                "total_chunks": len(chunks),
                "chunk_duration": chunk_duration,
                "estimated_processing_time": f"{max(len(chunks) / max_concurrent * 3, 5):.1f}s"
            }
            
            # Process chunks concurrently
            yield {"type": "status", "message": f"Processing {len(chunks)} chunks concurrently..."}
            
            async for result in self._process_chunks_concurrent(chunks, max_concurrent):
                yield result
                
            processing_time = time.time() - start_time
            speedup = video_info["duration"] / processing_time
            
            yield {
                "type": "processing_complete",
                "total_time": processing_time,
                "speedup_factor": speedup,
                "video_duration": video_info["duration"]
            }
            
        except Exception as e:
            logger.error(f"Fast transcription error: {e}")
            yield {"type": "error", "message": str(e)}
        finally:
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    async def _download_video(self, video_url: str, temp_dir: str) -> Tuple[Dict, str]:
        """Download video and extract audio"""
        
        def download_sync():
            # Get video info
            ydl_opts_info = {'quiet': True, 'no_warnings': True}
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                info = ydl.extract_info(video_url, download=False)
            
            # Download audio
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
            
            # Find audio file
            for file in os.listdir(temp_dir):
                if file.endswith('.wav'):
                    return info, os.path.join(temp_dir, file)
            
            raise FileNotFoundError("Audio file not found")
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, download_sync)

    async def _create_audio_chunks(
        self, 
        audio_file: str, 
        chunk_duration: float, 
        overlap: float,
        total_duration: float
    ) -> List[Dict[str, Any]]:
        """Create overlapping audio chunks for parallel processing"""
        
        def create_chunks_sync():
            # Load audio
            audio = AudioSegment.from_file(audio_file)
            audio = audio.set_frame_rate(16000).set_channels(1)
            
            chunks = []
            chunk_duration_ms = int(chunk_duration * 1000)
            overlap_ms = int(overlap * 1000)
            step_ms = chunk_duration_ms - overlap_ms
            
            chunk_id = 0
            for start_ms in range(0, len(audio), step_ms):
                end_ms = min(start_ms + chunk_duration_ms, len(audio))
                
                if end_ms - start_ms < 10000:  # Skip chunks shorter than 10 seconds
                    continue
                
                chunk = audio[start_ms:end_ms]
                chunk_file = os.path.join(os.path.dirname(audio_file), f"chunk_{chunk_id:04d}.wav")
                chunk.export(chunk_file, format="wav")
                
                chunks.append({
                    "id": chunk_id,
                    "file": chunk_file,
                    "start_time": start_ms / 1000.0,
                    "end_time": end_ms / 1000.0,
                    "duration": (end_ms - start_ms) / 1000.0
                })
                chunk_id += 1
            
            return chunks
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, create_chunks_sync)

    async def _process_chunks_concurrent(
        self, 
        chunks: List[Dict[str, Any]], 
        max_concurrent: int
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Process chunks concurrently using AssemblyAI API"""
        
        semaphore = asyncio.Semaphore(max_concurrent)
        completed = 0
        failed = 0
        results = []
        
        async def process_single_chunk(chunk: Dict[str, Any]) -> Optional[TranscriptResult]:
            async with semaphore:
                try:
                    start_time = time.time()
                    
                    # Upload and transcribe chunk
                    transcript = await self._transcribe_chunk(chunk["file"])
                    
                    processing_time = time.time() - start_time
                    
                    if transcript and transcript.strip():
                        return TranscriptResult(
                            chunk_id=chunk["id"],
                            start_time=chunk["start_time"],
                            end_time=chunk["end_time"],
                            text=transcript,
                            confidence=1.0,  # AssemblyAI doesn't return confidence for regular API
                            processing_time=processing_time
                        )
                    return None
                    
                except Exception as e:
                    logger.error(f"Error processing chunk {chunk['id']}: {e}")
                    return None
        
        # Create tasks for all chunks
        tasks = [process_single_chunk(chunk) for chunk in chunks]
        
        # Process chunks and yield results as they complete
        for coro in asyncio.as_completed(tasks):
            result = await coro
            
            if result:
                completed += 1
                results.append(result)
                
                yield {
                    "type": "chunk_complete",
                    "chunk_id": result.chunk_id,
                    "text": result.text,
                    "start_time": result.start_time,
                    "end_time": result.end_time,
                    "processing_time": result.processing_time
                }
            else:
                failed += 1
            
            # Progress update
            total_processed = completed + failed
            yield {
                "type": "progress",
                "completed": completed,
                "failed": failed,
                "total": len(chunks),
                "progress_percent": (total_processed / len(chunks)) * 100
            }
        
        # Sort results by start time and combine
        results.sort(key=lambda x: x.start_time)
        full_transcript = " ".join([r.text for r in results])
        
        yield {
            "type": "transcription_complete",
            "full_transcript": full_transcript,
            "total_chunks": len(chunks),
            "successful_chunks": completed,
            "failed_chunks": failed,
            "chunks": [
                {
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "text": r.text
                } for r in results
            ]
        }

    async def _transcribe_chunk(self, audio_file: str) -> Optional[str]:
        """Transcribe a single audio chunk using AssemblyAI"""
        
        try:
            # Upload audio file
            upload_url = await self._upload_audio(audio_file)
            
            # Submit transcription job
            transcript_id = await self._submit_transcription(upload_url)
            
            # Poll for completion
            transcript = await self._poll_transcription(transcript_id)
            
            return transcript
            
        except Exception as e:
            logger.error(f"Chunk transcription failed: {e}")
            return None

    async def _upload_audio(self, audio_file: str) -> str:
        """Upload audio file to AssemblyAI"""
        
        upload_url = "https://api.assemblyai.com/v2/upload"
        headers = {"authorization": self.api_key}
        
        with open(audio_file, 'rb') as f:
            async with self.session.post(upload_url, headers=headers, data=f) as response:
                if response.status == 200:
                    result = await response.json()
                    return result["upload_url"]
                else:
                    raise Exception(f"Upload failed: {response.status}")

    async def _submit_transcription(self, upload_url: str) -> str:
        """Submit transcription job"""
        
        transcript_url = "https://api.assemblyai.com/v2/transcript"
        headers = {
            "authorization": self.api_key,
            "content-type": "application/json"
        }
        
        data = {
            "audio_url": upload_url,
            "speech_model": "best"  # Use best model for accuracy
        }
        
        async with self.session.post(transcript_url, headers=headers, json=data) as response:
            if response.status == 200:
                result = await response.json()
                return result["id"]
            else:
                raise Exception(f"Transcription submission failed: {response.status}")

    async def _poll_transcription(self, transcript_id: str) -> str:
        """Poll for transcription completion with faster polling"""
        
        poll_url = f"https://api.assemblyai.com/v2/transcript/{transcript_id}"
        headers = {"authorization": self.api_key}
        
        # Start with faster polling
        poll_intervals = [0.5, 0.5, 1, 1, 1, 2, 2, 3]  # Aggressive initial polling
        poll_count = 0
        
        while True:
            async with self.session.get(poll_url, headers=headers) as response:
                if response.status == 200:
                    result = await response.json()
                    status = result["status"]
                    
                    if status == "completed":
                        return result["text"]
                    elif status == "error":
                        raise Exception(f"Transcription failed: {result.get('error')}")
                    
                    # Use faster polling intervals initially
                    interval = poll_intervals[min(poll_count, len(poll_intervals) - 1)]
                    await asyncio.sleep(interval)
                    poll_count += 1
                    
                    # Timeout after 30 seconds
                    if poll_count * 2 > 30:
                        raise Exception("Transcription timeout")
                else:
                    raise Exception(f"Polling failed: {response.status}")


# FastAPI Application
app = FastAPI(title="Fast Chunked Transcription Service", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "message": "Fast Chunked Transcription Service", 
        "version": "2.0.0",
        "target_performance": "5 seconds for 20-minute video",
        "method": "concurrent_chunk_processing"
    }

@app.get("/health")
async def health():
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    return {
        "status": "healthy",
        "assemblyai_configured": bool(api_key and api_key != "your_assemblyai_api_key_here"),
        "processing_method": "fast_concurrent_chunks"
    }

@app.websocket("/fast-transcribe")
async def websocket_fast_transcribe(websocket: WebSocket):
    """WebSocket endpoint for fast chunked transcription"""
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
        
        # Get API key
        api_key = os.getenv('ASSEMBLYAI_API_KEY')
        if not api_key:
            await websocket.send_json({
                "type": "error", 
                "message": "AssemblyAI API key not configured"
            })
            return
        
        # Parameters
        chunk_duration = data.get("chunk_duration_seconds", 30.0)
        max_concurrent = data.get("max_concurrent_chunks", 10)
        overlap = data.get("overlap_seconds", 2.0)
        
        # Process video
        async with FastChunkedTranscriptionService(api_key) as service:
            async for result in service.transcribe_video_fast(
                video_url,
                chunk_duration=chunk_duration,
                max_concurrent=max_concurrent,
                overlap_seconds=overlap
            ):
                await websocket.send_json(result)
                
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.send_json({
            "type": "error", 
            "message": f"Processing failed: {str(e)}"
        })
    finally:
        await websocket.close()

@app.post("/fast-transcribe")
async def http_fast_transcribe(request: FastTranscriptionRequest):
    """HTTP endpoint for fast chunked transcription"""
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    if not api_key:
        raise HTTPException(status_code=500, detail="AssemblyAI API key not configured")
    
    async def generate_stream():
        try:
            async with FastChunkedTranscriptionService(api_key) as service:
                async for result in service.transcribe_video_fast(
                    str(request.url),
                    chunk_duration=request.chunk_duration_seconds,
                    max_concurrent=request.max_concurrent_chunks,
                    overlap_seconds=request.overlap_seconds
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
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    if not api_key:
        print("⚠️  WARNING: ASSEMBLYAI_API_KEY not configured")
    else:
        print("✅ AssemblyAI API key configured")
    
    print("🚀 Starting Fast Chunked Transcription Service")
    print("🎯 Target: 5 seconds for 20-minute video")
    print("⚡ Method: Concurrent chunk processing")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0", 
        port=8002,
        reload=True,
        log_level="info"
    )