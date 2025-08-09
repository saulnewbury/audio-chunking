"""
Improved Local Whisper Transcription
Optimized for maximum speed with practical concurrency
Target: Under 10 seconds for 15-minute video
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
import threading
from pathlib import Path

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

class ImprovedWhisperRequest(BaseModel):
    url: HttpUrl
    chunk_duration_seconds: Optional[float] = 12.0  # Smaller chunks
    max_concurrent_chunks: Optional[int] = 16  # More concurrency
    model_size: Optional[str] = "tiny"
    optimization_level: Optional[str] = "max"  # max, balanced, quality

class ImprovedWhisperService:
    def __init__(self, model_size: str = "tiny"):
        self.model_size = model_size
        # Use more aggressive concurrency
        max_workers = min(20, (os.cpu_count() or 4) * 2)
        self.executor = ProcessPoolExecutor(max_workers=max_workers)
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown()

    async def ultra_fast_transcription(
        self,
        video_url: str,
        chunk_duration: float = 12.0,
        max_concurrent: int = 16,
        model_size: str = "tiny",
        optimization_level: str = "max"
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Ultra-fast transcription with improved concurrency"""
        
        temp_dir = None
        start_time = time.time()
        
        try:
            temp_dir = tempfile.mkdtemp(prefix="improved_whisper_")
            
            if not self._check_whisper_installed():
                yield {"type": "error", "message": "Whisper not installed. Run: pip install openai-whisper"}
                return
            
            yield {"type": "status", "message": "Starting optimized download..."}
            
            # Fast download with optimized settings
            video_info, audio_file = await self._fast_download_video(video_url, temp_dir, optimization_level)
            
            yield {
                "type": "video_info",
                "title": video_info["title"],
                "duration": video_info["duration"],
                "model_size": model_size,
                "optimization_level": optimization_level,
                "estimated_time": f"{video_info['duration'] / 120:.1f}s",  # Very optimistic estimate
                "max_concurrent": max_concurrent
            }
            
            yield {"type": "status", "message": "Creating optimized chunks..."}
            
            # Create chunks with overlap for better accuracy
            chunks = await self._create_optimized_chunks(audio_file, chunk_duration, video_info["duration"])
            
            yield {
                "type": "chunks_created",
                "total_chunks": len(chunks),
                "chunk_duration": chunk_duration,
                "estimated_processing": f"{len(chunks) / max_concurrent * 1.5:.1f}s"
            }
            
            yield {"type": "status", "message": f"Processing {len(chunks)} chunks with {max_concurrent} workers..."}
            
            # Process with improved concurrency and batching
            async for result in self._process_chunks_optimized(chunks, max_concurrent, model_size, optimization_level):
                yield result
                
            processing_time = time.time() - start_time
            speedup = video_info["duration"] / processing_time
            
            yield {
                "type": "performance",
                "total_time": processing_time,
                "speedup_factor": speedup,
                "target_achieved": processing_time <= 10.0,  # More realistic target
                "optimization_level": optimization_level
            }
            
        except Exception as e:
            logger.error(f"Improved transcription error: {e}")
            yield {"type": "error", "message": str(e)}
        finally:
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    async def _fast_download_video(self, video_url: str, temp_dir: str, optimization_level: str):
        """Optimized video download"""
        def download_sync():
            # Get info first
            ydl_opts_info = {'quiet': True, 'no_warnings': True}
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                info = ydl.extract_info(video_url, download=False)
            
            # Download with optimized settings
            if optimization_level == "max":
                # Fastest download - lower quality audio
                format_selector = 'worstaudio[ext=m4a]/worstaudio/worst'
                audio_quality = '64'
            else:
                # Balanced quality
                format_selector = 'bestaudio[ext=m4a]/bestaudio/best'
                audio_quality = '128'
            
            ydl_opts = {
                'format': format_selector,
                'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'wav',
                    'preferredquality': audio_quality,
                }],
                # Speed optimizations
                'concurrent_fragment_downloads': 4,
                'retries': 1,
                'fragment_retries': 1,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            
            # Find the downloaded file
            for file in os.listdir(temp_dir):
                if file.endswith('.wav'):
                    return info, os.path.join(temp_dir, file)
            
            raise FileNotFoundError("Audio file not found")
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=2) as executor:
            return await loop.run_in_executor(executor, download_sync)

    async def _create_optimized_chunks(self, audio_file: str, chunk_duration: float, total_duration: float):
        """Create optimized chunks with overlap"""
        def create_chunks_sync():
            audio = AudioSegment.from_file(audio_file)
            
            # Optimize chunk creation
            chunks = []
            chunk_duration_ms = int(chunk_duration * 1000)
            overlap_ms = 1000  # 1 second overlap for better accuracy
            
            chunk_id = 0
            for start_ms in range(0, len(audio), chunk_duration_ms - overlap_ms):
                end_ms = min(start_ms + chunk_duration_ms, len(audio))
                
                # Skip very short chunks
                if end_ms - start_ms < 3000:  # 3 seconds minimum
                    continue
                
                chunk = audio[start_ms:end_ms]
                
                # Optimize audio for Whisper (mono, 16kHz)
                chunk = chunk.set_frame_rate(16000).set_channels(1)
                
                chunk_file = os.path.join(os.path.dirname(audio_file), f"chunk_{chunk_id:04d}.wav")
                chunk.export(chunk_file, format="wav", parameters=["-ac", "1", "-ar", "16000"])
                
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

    async def _process_chunks_optimized(self, chunks: List[Dict], max_concurrent: int, model_size: str, optimization_level: str):
        """Process chunks with optimized concurrency and batching"""
        
        # Adaptive semaphore based on system resources
        semaphore = asyncio.Semaphore(max_concurrent)
        completed = 0
        failed = 0
        results = []
        
        # Batch processing for better efficiency
        batch_size = max(4, max_concurrent // 4)
        
        async def process_single_chunk(chunk: Dict) -> Optional[WhisperResult]:
            async with semaphore:
                try:
                    start_time = time.time()
                    
                    # Use optimized transcription
                    transcript = await self._whisper_transcribe_optimized(
                        chunk["file"], model_size, optimization_level
                    )
                    
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
        
        # Process in batches for better resource management
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            tasks = [process_single_chunk(chunk) for chunk in batch]
            
            # Process batch
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

    async def _whisper_transcribe_optimized(self, audio_file: str, model_size: str, optimization_level: str) -> str:
        """Optimized Whisper transcription"""
        def run_whisper():
            try:
                audio_file_abs = os.path.abspath(audio_file)
                output_dir = os.path.dirname(audio_file_abs)
                
                # Verify file exists
                if not os.path.exists(audio_file_abs):
                    return ""
                
                # Build optimized command
                cmd = [
                    'whisper', audio_file_abs,
                    '--model', model_size,
                    '--output_format', 'txt',
                    '--output_dir', output_dir,
                    '--language', 'en',
                    '--threads', '1',  # Single thread per process
                    '--verbose', 'False',
                ]
                
                # Optimization-specific settings
                if optimization_level == "max":
                    cmd.extend([
                        '--fp16', 'False',
                        '--best_of', '1',  # Fastest beam search
                        '--beam_size', '1',
                        '--temperature', '0',  # Deterministic
                    ])
                
                # Run with shorter timeout for faster chunks
                timeout = max(30, len(os.path.basename(audio_file)) * 2)
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
                
                if result.returncode == 0:
                    base_name = os.path.splitext(os.path.basename(audio_file))[0]
                    txt_file = os.path.join(output_dir, f"{base_name}.txt")
                    
                    if os.path.exists(txt_file):
                        with open(txt_file, 'r', encoding='utf-8') as f:
                            transcript = f.read().strip()
                        os.unlink(txt_file)  # Clean up immediately
                        return transcript
                
                return ""
                
            except subprocess.TimeoutExpired:
                logger.warning(f"Whisper timeout for {audio_file}")
                return ""
            except Exception as e:
                logger.error(f"Whisper error for {audio_file}: {e}")
                return ""
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, run_whisper)

    def _check_whisper_installed(self) -> bool:
        """Check if whisper CLI is installed"""
        try:
            result = subprocess.run(['whisper', '--help'], 
                                  capture_output=True, text=True, timeout=5)
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False


# FastAPI Application
app = FastAPI(title="Improved Local Whisper Ultra-Fast Transcription", version="6.0.0")

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
        "message": "Improved Local Whisper Ultra-Fast Transcription", 
        "version": "6.0.0",
        "method": "optimized_concurrent_whisper",
        "target": "Under 10 seconds for 15-minute video",
        "features": [
            "Optimized download settings",
            "Smaller chunks with overlap", 
            "Batch processing",
            "16+ concurrent workers",
            "Faster Whisper parameters"
        ],
        "cost": "FREE - No API costs!"
    }

@app.websocket("/improved-whisper")
async def websocket_improved_whisper(websocket: WebSocket):
    """Improved ultra-fast local Whisper transcription endpoint"""
    await websocket.accept()
    
    try:
        data = await websocket.receive_json()
        video_url = data.get("video_url")
        
        if not video_url:
            await websocket.send_json({"type": "error", "message": "video_url required"})
            return
        
        chunk_duration = data.get("chunk_duration_seconds", 12.0)
        max_concurrent = data.get("max_concurrent_chunks", 16)
        model_size = data.get("model_size", "tiny")
        optimization_level = data.get("optimization_level", "max")
        
        async with ImprovedWhisperService(model_size) as service:
            async for result in service.ultra_fast_transcription(
                video_url, chunk_duration, max_concurrent, model_size, optimization_level
            ):
                await websocket.send_json(result)
                
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.send_json({"type": "error", "message": str(e)})
    finally:
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Improved Local Whisper Ultra-Fast Transcription")
    print("⚡ Method: Optimized concurrent processing")
    print("🎯 Target: Under 10 seconds for 15-minute video")
    print("💰 Cost: FREE - No API fees!")
    print("✨ Features: 16+ workers, optimized chunks, batch processing")
    print("📋 Requirements: pip install openai-whisper")
    
    uvicorn.run("local_whisper_service:app", host="0.0.0.0", port=8007, reload=False)