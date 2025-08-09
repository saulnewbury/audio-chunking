"""
Optimized Original Whisper Transcription
Based on your working code, just with better optimizations
Target: Cut your 76 seconds down to 25-35 seconds
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class WhisperResult:
    chunk_id: int
    start_time: float
    end_time: float
    text: str
    processing_time: float

class OptimizedRequest(BaseModel):
    url: HttpUrl
    chunk_duration_seconds: Optional[float] = 8.0
    max_concurrent_chunks: Optional[int] = 20
    model_size: Optional[str] = "tiny"

class OptimizedOriginalWhisperService:
    def __init__(self, model_size: str = "tiny"):
        self.model_size = model_size
        # Conservative but effective concurrency
        max_workers = min(25, (os.cpu_count() or 4) * 3)
        self.executor = ProcessPoolExecutor(max_workers=max_workers)
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown()

    async def optimized_transcription(
        self,
        video_url: str,
        chunk_duration: float = 8.0,
        max_concurrent: int = 20,
        model_size: str = "tiny"
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Optimized version of your working approach"""
        
        temp_dir = None
        start_time = time.time()
        
        try:
            temp_dir = tempfile.mkdtemp(prefix="optimized_whisper_")
            
            if not self._check_whisper_installed():
                yield {"type": "error", "message": "Whisper not installed. Run: pip install openai-whisper"}
                return
            
            yield {"type": "status", "message": "Getting video info..."}
            
            # Step 1: Get video info (same as your working code)
            video_info = await self._get_video_info_fast(video_url)
            
            yield {
                "type": "video_info",
                "title": video_info["title"],
                "duration": video_info["duration"],
                "model_size": model_size,
                "optimization_level": "improved",
                "estimated_time": f"{video_info['duration'] / 80:.1f}s",
                "max_concurrent": max_concurrent
            }
            
            # Step 2: Faster download (optimized from your working version)
            yield {"type": "status", "message": "Downloading with optimized settings..."}
            download_start = time.time()
            
            video_info, audio_file = await self._download_video_optimized(video_url, temp_dir)
            download_time = time.time() - download_start
            
            yield {
                "type": "download_complete",
                "download_time": download_time,
                "message": f"Downloaded in {download_time:.1f}s"
            }
            
            # Step 3: Create smaller chunks (optimized from your working version)
            yield {"type": "status", "message": "Creating optimized chunks..."}
            chunk_start = time.time()
            
            chunks = await self._create_chunks_optimized(audio_file, chunk_duration, video_info["duration"])
            chunk_time = time.time() - chunk_start
            
            yield {
                "type": "chunks_created",
                "total_chunks": len(chunks),
                "chunk_creation_time": chunk_time,
                "estimated_processing": f"{len(chunks) / max_concurrent * 1.2:.1f}s"
            }
            
            # Step 4: Process with better concurrency (optimized from your working version)
            yield {"type": "status", "message": f"Processing {len(chunks)} chunks with {max_concurrent} workers..."}
            
            processing_start = time.time()
            async for result in self._process_chunks_optimized(chunks, max_concurrent, model_size):
                yield result
            
            processing_time = time.time() - processing_start
            total_time = time.time() - start_time
            speedup = video_info["duration"] / total_time
            
            yield {
                "type": "performance",
                "total_time": total_time,
                "download_time": download_time,
                "chunk_creation_time": chunk_time,
                "processing_time": processing_time,
                "speedup_factor": speedup,
                "target_achieved": total_time <= 35.0
            }
                
        except Exception as e:
            logger.error(f"Optimized transcription error: {e}", exc_info=True)
            yield {"type": "error", "message": f"Error: {str(e)}"}
        finally:
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    async def _get_video_info_fast(self, video_url: str):
        """Get video info quickly"""
        def get_info():
            ydl_opts_info = {'quiet': True, 'no_warnings': True}
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                return ydl.extract_info(video_url, download=False)
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, get_info)

    async def _download_video_optimized(self, video_url: str, temp_dir: str):
        """Optimized download - faster than your original"""
        def download_sync():
            # Get info first
            ydl_opts_info = {'quiet': True, 'no_warnings': True}
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                info = ydl.extract_info(video_url, download=False)
            
            # Optimized download settings
            ydl_opts = {
                'format': 'worstaudio[ext=m4a]/worstaudio/worst',  # Fastest format
                'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
                'quiet': True,
                'no_warnings': True,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'wav',
                    'preferredquality': '96',  # Lower quality for speed
                }],
                # Speed optimizations (conservative but effective)
                'concurrent_fragment_downloads': 8,
                'retries': 1,
                'fragment_retries': 1,
                'http_chunk_size': 10485760,  # 10MB chunks
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            
            # Find the audio file
            for file in os.listdir(temp_dir):
                if file.endswith('.wav'):
                    return info, os.path.join(temp_dir, file)
            
            raise FileNotFoundError("Audio file not found")
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, download_sync)

    async def _create_chunks_optimized(self, audio_file: str, chunk_duration: float, total_duration: float):
        """Create chunks faster than your original"""
        def create_chunks_sync():
            audio = AudioSegment.from_file(audio_file)
            
            # Optimize for Whisper immediately
            audio = audio.set_frame_rate(16000).set_channels(1)
            
            chunks = []
            chunk_duration_ms = int(chunk_duration * 1000)
            overlap_ms = 500  # Smaller overlap for speed
            
            chunk_id = 0
            for start_ms in range(0, len(audio), chunk_duration_ms - overlap_ms):
                end_ms = min(start_ms + chunk_duration_ms, len(audio))
                
                if end_ms - start_ms < 3000:  # Skip very small chunks
                    continue
                
                chunk = audio[start_ms:end_ms]
                chunk_file = os.path.join(os.path.dirname(audio_file), f"chunk_{chunk_id:04d}.wav")
                
                # Faster export
                chunk.export(
                    chunk_file, 
                    format="wav",
                    parameters=["-ac", "1", "-ar", "16000"]
                )
                
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

    async def _process_chunks_optimized(self, chunks: List[Dict], max_concurrent: int, model_size: str):
        """Process chunks with better concurrency than your original"""
        
        semaphore = asyncio.Semaphore(max_concurrent)
        completed = 0
        failed = 0
        results = []
        first_result_time = None
        processing_start = time.time()
        
        async def process_single_chunk(chunk: Dict) -> Optional[WhisperResult]:
            async with semaphore:
                try:
                    start_time = time.time()
                    
                    transcript = await self._whisper_transcribe_optimized(
                        chunk["file"], model_size
                    )
                    
                    processing_time = time.time() - start_time
                    
                    # Clean up immediately
                    try:
                        os.unlink(chunk["file"])
                    except:
                        pass
                    
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
                
                # Track first result
                if first_result_time is None:
                    first_result_time = time.time()
                    time_to_first = first_result_time - processing_start
                    yield {
                        "type": "first_result",
                        "time_to_first_result": time_to_first,
                        "message": f"First result in {time_to_first:.1f}s!"
                    }
                
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
        
        # Final results
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

    async def _whisper_transcribe_optimized(self, audio_file: str, model_size: str) -> str:
        """Optimized Whisper transcription"""
        def run_whisper():
            try:
                audio_file_abs = os.path.abspath(audio_file)
                output_dir = os.path.dirname(audio_file_abs)
                
                # Optimized command (same as your working version but faster)
                cmd = [
                    'whisper', audio_file_abs,
                    '--model', model_size,
                    '--output_format', 'txt',
                    '--output_dir', output_dir,
                    '--verbose', 'False',
                    '--language', 'en',
                    '--fp16', 'False',
                    '--threads', '1',
                    '--best_of', '1',
                    '--beam_size', '1',
                    '--temperature', '0'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    base_name = os.path.splitext(os.path.basename(audio_file))[0]
                    txt_file = os.path.join(output_dir, f"{base_name}.txt")
                    
                    if os.path.exists(txt_file):
                        with open(txt_file, 'r', encoding='utf-8') as f:
                            transcript = f.read().strip()
                        os.unlink(txt_file)
                        return transcript
                
                return ""
                
            except subprocess.TimeoutExpired:
                logger.warning(f"Whisper timeout for {audio_file}")
                return ""
            except Exception as e:
                logger.error(f"Whisper error: {e}")
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
app = FastAPI(title="Optimized Original Whisper Transcription", version="13.0.0")

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
        "message": "Optimized Original Whisper Transcription", 
        "version": "13.0.0",
        "method": "optimized_original",
        "target": "Cut 76s down to 25-35s",
        "features": [
            "Based on your working code",
            "Faster download settings",
            "Better chunk processing",
            "Conservative but effective optimizations"
        ],
        "cost": "FREE - No API costs!",
        "endpoints": {
            "websocket": "/optimized-whisper",
            "health": "/health"
        }
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": time.time()}

@app.websocket("/optimized-whisper")
async def websocket_optimized_whisper(websocket: WebSocket):
    """Optimized Whisper transcription endpoint"""
    await websocket.accept()
    
    try:
        data = await websocket.receive_json()
        video_url = data.get("video_url")
        
        if not video_url:
            await websocket.send_json({"type": "error", "message": "video_url required"})
            return
        
        chunk_duration = data.get("chunk_duration_seconds", 8.0)
        max_concurrent = data.get("max_concurrent_chunks", 20)
        model_size = data.get("model_size", "tiny")
        
        logger.info(f"Starting optimized transcription for: {video_url}")
        logger.info(f"Settings: chunks={chunk_duration}s, workers={max_concurrent}, model={model_size}")
        
        async with OptimizedOriginalWhisperService(model_size) as service:
            async for result in service.optimized_transcription(
                video_url, chunk_duration, max_concurrent, model_size
            ):
                await websocket.send_json(result)
                
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
        await websocket.send_json({"type": "error", "message": str(e)})
    finally:
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Optimized Original Whisper Transcription")
    print("⚡ Method: Your working code + optimizations")
    print("🎯 Target: Cut your 76s down to 25-35s")
    print("💰 Cost: FREE - No API fees!")
    print("✅ Based on your proven working approach")
    print("📋 Requirements: pip install openai-whisper")
    
    uvicorn.run("optimized_original_whisper:app", host="0.0.0.0", port=8014, reload=False)