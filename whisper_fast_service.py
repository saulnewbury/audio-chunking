"""
Ultra-Fast Transcription with OpenAI Whisper API
Target: 5 seconds for 20-minute video
"""

import asyncio
import os
import tempfile
import logging
import time
import shutil
from typing import AsyncGenerator, Dict, Any, List, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import aiofiles

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl

import yt_dlp
from pydub import AudioSegment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()

@dataclass
class WhisperResult:
    chunk_id: int
    start_time: float
    end_time: float
    text: str
    processing_time: float

class WhisperFastRequest(BaseModel):
    url: HttpUrl
    chunk_duration_seconds: Optional[float] = 60.0
    max_concurrent_chunks: Optional[int] = 20

class WhisperFastService:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session: aiohttp.ClientSession = None
        self.executor = ThreadPoolExecutor(max_workers=4)
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fast_transcription(
        self,
        video_url: str,
        chunk_duration: float = 60.0,
        max_concurrent: int = 20
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Ultra-fast transcription using OpenAI Whisper API"""
        
        temp_dir = None
        start_time = time.time()
        
        try:
            temp_dir = tempfile.mkdtemp(prefix="whisper_fast_")
            
            # Download
            yield {"type": "status", "message": "Downloading video..."}
            video_info, audio_file = await self._download_video(video_url, temp_dir)
            
            yield {
                "type": "video_info",
                "title": video_info["title"],
                "duration": video_info["duration"],
                "estimated_time": f"{video_info['duration'] / 60:.1f}s"  # Whisper is very fast
            }
            
            # Create chunks
            yield {"type": "status", "message": "Creating audio chunks..."}
            chunks = await self._create_chunks(audio_file, chunk_duration, video_info["duration"])
            
            yield {
                "type": "chunks_created",
                "total_chunks": len(chunks),
                "estimated_processing": f"{len(chunks) / max_concurrent * 2:.1f}s"
            }
            
            # Process all chunks concurrently
            yield {"type": "status", "message": f"Processing {len(chunks)} chunks with Whisper API..."}
            
            async for result in self._process_chunks_whisper(chunks, max_concurrent):
                yield result
                
            processing_time = time.time() - start_time
            speedup = video_info["duration"] / processing_time
            
            yield {
                "type": "performance",
                "total_time": processing_time,
                "speedup_factor": speedup,
                "target_achieved": processing_time <= 5.0
            }
            
        except Exception as e:
            logger.error(f"Whisper transcription error: {e}")
            yield {"type": "error", "message": str(e)}
        finally:
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    async def _download_video(self, video_url: str, temp_dir: str):
        """Download and extract audio"""
        def download_sync():
            ydl_opts_info = {'quiet': True, 'no_warnings': True}
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                info = ydl.extract_info(video_url, download=False)
            
            ydl_opts = {
                'format': 'bestaudio[ext=m4a]/bestaudio/best',
                'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
                'quiet': True,
                'no_warnings': True,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',  # Whisper handles MP3 well
                    'preferredquality': '192',
                }],
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            
            for file in os.listdir(temp_dir):
                if file.endswith('.mp3'):
                    return info, os.path.join(temp_dir, file)
            
            raise FileNotFoundError("Audio file not found")
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, download_sync)

    async def _create_chunks(self, audio_file: str, chunk_duration: float, total_duration: float):
        """Create audio chunks for parallel processing"""
        def create_chunks_sync():
            audio = AudioSegment.from_file(audio_file)
            
            chunks = []
            chunk_duration_ms = int(chunk_duration * 1000)
            
            chunk_id = 0
            for start_ms in range(0, len(audio), chunk_duration_ms):
                end_ms = min(start_ms + chunk_duration_ms, len(audio))
                
                if end_ms - start_ms < 10000:  # Skip chunks shorter than 10 seconds
                    continue
                
                chunk = audio[start_ms:end_ms]
                chunk_file = os.path.join(os.path.dirname(audio_file), f"chunk_{chunk_id:04d}.mp3")
                chunk.export(chunk_file, format="mp3")
                
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

    async def _process_chunks_whisper(self, chunks: List[Dict], max_concurrent: int):
        """Process chunks concurrently with Whisper API"""
        
        semaphore = asyncio.Semaphore(max_concurrent)
        completed = 0
        failed = 0
        results = []
        
        async def process_single_chunk(chunk: Dict) -> Optional[WhisperResult]:
            async with semaphore:
                try:
                    start_time = time.time()
                    
                    # Transcribe with Whisper API
                    transcript = await self._whisper_transcribe(chunk["file"])
                    
                    processing_time = time.time() - start_time
                    
                    if transcript and transcript.strip():
                        return WhisperResult(
                            chunk_id=chunk["id"],
                            start_time=chunk["start_time"],
                            end_time=chunk["end_time"],
                            text=transcript.strip(),
                            processing_time=processing_time
                        )
                    return None
                    
                except Exception as e:
                    logger.error(f"Error processing chunk {chunk['id']}: {e}")
                    # Add more detailed error logging
                    import traceback
                    logger.error(f"Full error traceback: {traceback.format_exc()}")
                    return None
        
        # Process all chunks concurrently
        tasks = [process_single_chunk(chunk) for chunk in chunks]
        
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
            
            # Progress
            total_processed = completed + failed
            yield {
                "type": "progress",
                "completed": completed,
                "failed": failed,
                "total": len(chunks),
                "progress_percent": (total_processed / len(chunks)) * 100
            }
        
        # Combine results
        results.sort(key=lambda x: x.start_time)
        full_transcript = " ".join([r.text for r in results])
        
        yield {
            "type": "transcription_complete",
            "full_transcript": full_transcript,
            "total_chunks": len(chunks),
            "successful_chunks": completed,
            "failed_chunks": failed,
            "average_chunk_time": sum(r.processing_time for r in results) / len(results) if results else 0
        }

    async def _whisper_transcribe(self, audio_file: str) -> str:
        """Transcribe audio file using OpenAI Whisper API"""
        
        url = "https://api.openai.com/v1/audio/transcriptions"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        
        # Check if file exists and get size
        if not os.path.exists(audio_file):
            raise Exception(f"Audio file not found: {audio_file}")
        
        file_size = os.path.getsize(audio_file)
        logger.info(f"Transcribing {audio_file} ({file_size} bytes)")
        
        # Read file into memory first (fixes the closed file issue)
        with open(audio_file, 'rb') as f:
            audio_data = f.read()
        
        # Create form data
        data = aiohttp.FormData()
        data.add_field('model', 'whisper-1')
        data.add_field('response_format', 'text')
        data.add_field('file', audio_data, filename=os.path.basename(audio_file), content_type='audio/mpeg')
        
        try:
            async with self.session.post(url, headers=headers, data=data, timeout=30) as response:
                response_text = await response.text()
                
                if response.status == 200:
                    logger.info(f"Successfully transcribed {audio_file}")
                    return response_text.strip()
                else:
                    logger.error(f"Whisper API error {response.status}: {response_text}")
                    raise Exception(f"Whisper API error {response.status}: {response_text}")
                    
        except asyncio.TimeoutError:
            raise Exception(f"Whisper API timeout for {audio_file}")
        except Exception as e:
            logger.error(f"Whisper API request failed: {e}")
            raise


# FastAPI Application
app = FastAPI(title="Whisper Ultra-Fast Transcription", version="4.0.0")

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
        "message": "Whisper Ultra-Fast Transcription", 
        "version": "4.0.0",
        "method": "openai_whisper_concurrent",
        "target": "5 seconds for 20-minute video"
    }

@app.websocket("/whisper-fast")
async def websocket_whisper_fast(websocket: WebSocket):
    """Ultra-fast Whisper transcription endpoint"""
    await websocket.accept()
    
    try:
        data = await websocket.receive_json()
        video_url = data.get("video_url")
        
        if not video_url:
            await websocket.send_json({"type": "error", "message": "video_url required"})
            return
        
        # Use OpenAI API key
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            await websocket.send_json({"type": "error", "message": "OPENAI_API_KEY not configured"})
            return
        
        chunk_duration = data.get("chunk_duration_seconds", 60.0)
        max_concurrent = data.get("max_concurrent_chunks", 20)
        
        async with WhisperFastService(api_key) as service:
            async for result in service.fast_transcription(video_url, chunk_duration, max_concurrent):
                await websocket.send_json(result)
                
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.send_json({"type": "error", "message": str(e)})
    finally:
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    
    openai_key = os.getenv('OPENAI_API_KEY')
    if not openai_key:
        print("⚠️  WARNING: OPENAI_API_KEY not configured in .env")
        print("Add: OPENAI_API_KEY=your_key_here")
    else:
        print("✅ OpenAI API key configured")
    
    print("🚀 Starting Whisper Ultra-Fast Transcription")
    print("⚡ Method: OpenAI Whisper API with concurrency")
    print("🎯 Target: 5 seconds for 20-minute video")
    
    uvicorn.run("whisper_fast_service:app", host="0.0.0.0", port=8005, reload=False)