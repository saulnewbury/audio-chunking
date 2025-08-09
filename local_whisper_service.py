"""
Ultra-Fast Local Whisper Transcription
Uses local Whisper model for maximum speed
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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import subprocess

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl

import yt_dlp
from pydub import AudioSegment

logging.basicConfig(level=logging.DEBUG)
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

class LocalWhisperRequest(BaseModel):
    url: HttpUrl
    chunk_duration_seconds: Optional[float] = 60.0
    max_concurrent_chunks: Optional[int] = 4  # Limited by CPU cores
    model_size: Optional[str] = "base"  # tiny, base, small, medium, large

class LocalWhisperService:
    def __init__(self, model_size: str = "base"):
        self.model_size = model_size
        self.executor = ProcessPoolExecutor(max_workers=4)  # Use processes for CPU-bound work
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown()

    async def fast_transcription(
        self,
        video_url: str,
        chunk_duration: float = 60.0,
        max_concurrent: int = 4,
        model_size: str = "base"
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Ultra-fast transcription using local Whisper"""
        
        temp_dir = None
        start_time = time.time()
        
        try:
            temp_dir = tempfile.mkdtemp(prefix="local_whisper_")
            
            # Check if whisper is installed
            if not self._check_whisper_installed():
                yield {"type": "error", "message": "Whisper not installed. Run: pip install openai-whisper"}
                return
            
            # Download
            yield {"type": "status", "message": "Downloading video..."}
            video_info, audio_file = await self._download_video(video_url, temp_dir)
            
            yield {
                "type": "video_info",
                "title": video_info["title"],
                "duration": video_info["duration"],
                "model_size": model_size,
                "estimated_time": f"{video_info['duration'] / 60:.1f}s"  # Local Whisper is very fast
            }
            
            # Create chunks
            yield {"type": "status", "message": "Creating audio chunks..."}
            chunks = await self._create_chunks(audio_file, chunk_duration, video_info["duration"])
            
            yield {
                "type": "chunks_created",
                "total_chunks": len(chunks),
                "estimated_processing": f"{len(chunks) / max_concurrent * 3:.1f}s"
            }
            
            # Process all chunks concurrently
            yield {"type": "status", "message": f"Processing {len(chunks)} chunks with local Whisper ({model_size})..."}
            
            async for result in self._process_chunks_whisper(chunks, max_concurrent, model_size):
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
            logger.error(f"Local Whisper transcription error: {e}")
            yield {"type": "error", "message": str(e)}
        finally:
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    def _check_whisper_installed(self) -> bool:
        """Check if whisper CLI is installed"""
        try:
            result = subprocess.run(['whisper', '--help'], 
                                  capture_output=True, text=True, timeout=5)
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

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
                    'preferredcodec': 'wav',  # Whisper prefers WAV
                    'preferredquality': '192',
                }],
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            
            for file in os.listdir(temp_dir):
                if file.endswith('.wav'):
                    return info, os.path.join(temp_dir, file)
            
            raise FileNotFoundError("Audio file not found")
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, download_sync)

    async def _create_chunks(self, audio_file: str, chunk_duration: float, total_duration: float):
        """Create audio chunks for parallel processing"""
        def create_chunks_sync():
            audio = AudioSegment.from_file(audio_file)
            
            chunks = []
            chunk_duration_ms = int(chunk_duration * 1000)
            
            chunk_id = 0
            for start_ms in range(0, len(audio), chunk_duration_ms):
                end_ms = min(start_ms + chunk_duration_ms, len(audio))
                
                if end_ms - start_ms < 5000:  # Skip chunks shorter than 5 seconds
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
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, create_chunks_sync)

    async def _process_chunks_whisper(self, chunks: List[Dict], max_concurrent: int, model_size: str):
        """Process chunks concurrently with local Whisper"""
        
        semaphore = asyncio.Semaphore(max_concurrent)
        completed = 0
        failed = 0
        results = []
        
        async def process_single_chunk(chunk: Dict) -> Optional[WhisperResult]:
            async with semaphore:
                try:
                    start_time = time.time()
                    
                    # Transcribe with local Whisper
                    transcript = await self._whisper_transcribe_local(chunk["file"], model_size)
                    
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

    async def _whisper_transcribe_local(self, audio_file: str, model_size: str) -> str:
      """Transcribe audio file using local Whisper CLI"""
      def run_whisper():
        try:
            audio_file_abs = os.path.abspath(audio_file)
            output_dir = os.path.dirname(audio_file_abs)
            logger.debug(f"Running Whisper on {audio_file_abs} with model {model_size}")
            
            # Verify audio file exists and is readable
            if not os.path.exists(audio_file_abs):
                logger.error(f"Audio file does not exist: {audio_file_abs}")
                return ""
            if not os.access(audio_file_abs, os.R_OK):
                logger.error(f"Audio file not readable: {audio_file_abs}")
                return ""
            
            # Ensure output directory is writable
            if not os.access(output_dir, os.W_OK):
                logger.error(f"Output directory not writable: {output_dir}")
                return ""
            
            cmd = [
                'whisper', audio_file_abs,
                '--model', model_size,
                '--output_format', 'txt',
                '--output_dir', output_dir,
                '--verbose', 'True',
                '--language', 'en',
                '--fp16', 'False'
            ]
            logger.debug(f"Executing command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            logger.debug(f"Whisper exit code: {result.returncode}")
            logger.debug(f"Whisper stdout: {result.stdout}")
            logger.debug(f"Whisper stderr: {result.stderr}")
            
            if result.returncode == 0:
                base_name = os.path.splitext(os.path.basename(audio_file))[0]
                txt_file = os.path.join(output_dir, f"{base_name}.txt")
                logger.debug(f"Looking for output file: {txt_file}")
                
                if os.path.exists(txt_file):
                    with open(txt_file, 'r', encoding='utf-8') as f:
                        transcript = f.read().strip()
                    logger.debug(f"Transcript: {transcript[:200]}...")
                    os.remove(txt_file)
                    return transcript
                else:
                    logger.error(f"Whisper output file not found: {txt_file}")
                    return ""
            else:
                logger.error(f"Whisper failed with exit code {result.returncode}: {result.stderr}")
                return ""
        except subprocess.TimeoutExpired as e:
            logger.error(f"Whisper timeout for {audio_file_abs}: {str(e)}")
            return ""
        except Exception as e:
            logger.error(f"Whisper execution error for {audio_file_abs}: {str(e)}")
            return ""
    
      loop = asyncio.get_event_loop()
      with ThreadPoolExecutor() as executor:  # Use ThreadPoolExecutor
         return await loop.run_in_executor(executor, run_whisper)


# FastAPI Application
app = FastAPI(title="Local Whisper Ultra-Fast Transcription", version="5.0.0")

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
        "message": "Local Whisper Ultra-Fast Transcription", 
        "version": "5.0.0",
        "method": "local_whisper_concurrent",
        "target": "5 seconds for 20-minute video",
        "cost": "FREE - No API costs!"
    }

@app.websocket("/local-whisper")
async def websocket_local_whisper(websocket: WebSocket):
    """Ultra-fast local Whisper transcription endpoint"""
    await websocket.accept()
    
    try:
        data = await websocket.receive_json()
        video_url = data.get("video_url")
        
        if not video_url:
            await websocket.send_json({"type": "error", "message": "video_url required"})
            return
        
        chunk_duration = data.get("chunk_duration_seconds", 60.0)
        max_concurrent = data.get("max_concurrent_chunks", 4)
        model_size = data.get("model_size", "base")
        
        async with LocalWhisperService(model_size) as service:
            async for result in service.fast_transcription(video_url, chunk_duration, max_concurrent, model_size):
                await websocket.send_json(result)
                
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.send_json({"type": "error", "message": str(e)})
    finally:
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Local Whisper Ultra-Fast Transcription")
    print("⚡ Method: Local Whisper with multiprocessing")
    print("🎯 Target: 5 seconds for 20-minute video")
    print("💰 Cost: FREE - No API fees!")
    print("📋 Requirements: pip install openai-whisper")
    
    uvicorn.run("local_whisper_service:app", host="0.0.0.0", port=8006, reload=False)