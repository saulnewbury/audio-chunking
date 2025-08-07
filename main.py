# Optimized chunking service with performance improvements
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, HttpUrl
from typing import Optional, List, Dict, Any
import asyncio
import tempfile
import os
import logging
import time
import uuid
import io
import json
import shutil
from concurrent.futures import ThreadPoolExecutor
import yt_dlp
from pydub import AudioSegment
import assemblyai as aai
import math

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Optimized AssemblyAI Chunking Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://your-domain.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Performance-focused configuration
CHUNK_THRESHOLD_MINUTES = 15  # Only chunk videos longer than 15 minutes
MAX_CONCURRENT_CHUNKS = 5     # Increased concurrency
CHUNK_SIZE_MINUTES = 3        # Smaller chunks for better parallelization
OVERLAP_SECONDS = 0.5         # Reduced overlap

class ChunkingRequest(BaseModel):
    url: HttpUrl
    chunk_size_minutes: Optional[int] = CHUNK_SIZE_MINUTES
    max_concurrent: Optional[int] = MAX_CONCURRENT_CHUNKS
    language_code: Optional[str] = "en_us"

class ChunkProgress(BaseModel):
    chunk_id: int
    status: str
    start_time: float
    end_time: float
    text: Optional[str] = None
    error: Optional[str] = None
    processing_time: Optional[float] = None

class TranscriptResponse(BaseModel):
    text: str
    status: str
    video_title: str
    total_duration: float
    total_chunks: int
    processing_time: float
    service_method: str
    chunks: List[Dict[str, Any]]

# In-memory storage for job progress
job_progress: Dict[str, Dict] = {}

def extract_video_id(url: str) -> str:
    """Extract video ID from YouTube URL"""
    import re
    patterns = [
        r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([^&\n?#]+)',
        r'youtube\.com\/v\/([^&\n?#]+)',
        r'youtube\.com\/.*[?&]v=([^&\n?#]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    raise ValueError("Could not extract video ID from URL")

def get_video_info_and_audio(url: str) -> tuple[str, str, float]:
    """Download video info and extract audio file"""
    try:
        temp_dir = tempfile.mkdtemp(prefix="yt_audio_")
        safe_filename = f"audio_{uuid.uuid4().hex[:8]}"
        
        ydl_opts = {
            'format': 'bestaudio[ext=mp3]/bestaudio/best',
            'outtmpl': os.path.join(temp_dir, f'{safe_filename}.%(ext)s'),
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '128',
            }],
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            logger.info("Getting video info...")
            info = ydl.extract_info(url, download=False)
            title = info.get('title', 'YouTube Video')
            duration = info.get('duration', 0)
            
            if duration > 10800:
                raise ValueError("Video is too long (>3 hours). Please try a shorter video.")
            
            logger.info(f"Video: {title}, Duration: {duration}s")
            
            logger.info("Downloading and converting audio...")
            ydl.download([url])
            
            audio_file = None
            for file in os.listdir(temp_dir):
                if file.startswith(safe_filename) and file.endswith('.mp3'):
                    audio_file = os.path.join(temp_dir, file)
                    break
            
            if not audio_file:
                for file in os.listdir(temp_dir):
                    if any(file.endswith(ext) for ext in ['.mp3', '.m4a', '.webm', '.wav']):
                        audio_file = os.path.join(temp_dir, file)
                        if not audio_file.endswith('.mp3'):
                            import subprocess
                            mp3_file = audio_file.replace(os.path.splitext(audio_file)[1], '.mp3')
                            subprocess.run([
                                'ffmpeg', '-i', audio_file, '-acodec', 'mp3', '-ab', '128k', mp3_file
                            ], check=True, capture_output=True)
                            os.remove(audio_file)
                            audio_file = mp3_file
                        break
            
            if not audio_file or not os.path.exists(audio_file):
                files_in_temp = os.listdir(temp_dir) if os.path.exists(temp_dir) else []
                raise FileNotFoundError(f"Audio file not found. Files in temp dir: {files_in_temp}")
            
            if os.path.getsize(audio_file) == 0:
                raise ValueError("Downloaded audio file is empty")
            
            logger.info(f"Audio file ready: {audio_file} ({os.path.getsize(audio_file)} bytes)")
            return title, audio_file, duration
            
    except Exception as e:
        logger.error(f"Error extracting audio: {e}")
        if 'temp_dir' in locals() and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
        raise HTTPException(status_code=400, detail=f"Failed to extract audio: {str(e)}")

class OptimizedChunkingService:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CHUNKS)
    
    def should_use_chunking(self, duration_seconds: float) -> bool:
        """Determine if chunking is beneficial for this video length"""
        return duration_seconds > (CHUNK_THRESHOLD_MINUTES * 60)
    
    def create_chunks_in_memory(self, audio_file: str, chunk_size_minutes: int) -> List[tuple[bytes, float, float]]:
        """Create audio chunks in memory without disk I/O"""
        start_time = time.time()
        
        try:
            # Load audio once
            audio = AudioSegment.from_file(audio_file)
            audio_duration = len(audio) / 1000.0
            chunk_size_seconds = chunk_size_minutes * 60
            
            logger.info(f"Creating chunks in memory for {audio_duration:.1f}s audio")
            
            chunks = []
            current_start = 0.0
            chunk_id = 0
            
            while current_start < audio_duration:
                end_time = min(current_start + chunk_size_seconds, audio_duration)
                
                # Skip tiny chunks
                if end_time - current_start < 10.0:
                    break
                
                # Extract chunk segment
                start_ms = int(current_start * 1000)
                end_ms = int(end_time * 1000)
                chunk_audio = audio[start_ms:end_ms]
                
                # Export to bytes buffer (no disk I/O)
                buffer = io.BytesIO()
                chunk_audio.export(buffer, format="wav")  # WAV is faster than MP3
                chunk_bytes = buffer.getvalue()
                
                chunks.append((chunk_bytes, current_start, end_time))
                
                logger.info(f"Created chunk {chunk_id}: {current_start:.1f}s - {end_time:.1f}s ({len(chunk_bytes)} bytes)")
                
                # Move to next position
                current_start += chunk_size_seconds - OVERLAP_SECONDS
                chunk_id += 1
            
            processing_time = time.time() - start_time
            logger.info(f"Created {len(chunks)} chunks in {processing_time:.2f}s")
            
            return chunks
            
        except Exception as e:
            logger.error(f"Error creating chunks: {e}")
            raise
    
    async def transcribe_chunk_from_memory(self, chunk_data: bytes, chunk_id: int, 
                                         start_time: float, end_time: float) -> dict:
        """Transcribe chunk directly from memory"""
        chunk_start = time.time()
        
        try:
            logger.info(f"Transcribing chunk {chunk_id} ({start_time:.1f}s - {end_time:.1f}s)")
            
            # Create transcriber and upload chunk
            transcriber = aai.Transcriber()
            
            # Upload chunk bytes directly using the files API
            upload_url = await asyncio.get_event_loop().run_in_executor(
                self.executor, 
                lambda: aai.upload(chunk_data)  # Use the direct upload function
            )
            
            # Transcribe
            transcript = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                lambda: transcriber.transcribe(upload_url)
            )
            
            processing_time = time.time() - chunk_start
            
            if transcript.status == aai.TranscriptStatus.error:
                logger.error(f"Chunk {chunk_id} failed: {transcript.error}")
                return {
                    "chunk_id": chunk_id,
                    "status": "error",
                    "start_time": start_time,
                    "end_time": end_time,
                    "error": transcript.error,
                    "processing_time": processing_time
                }
            
            logger.info(f"Chunk {chunk_id} completed in {processing_time:.2f}s")
            return {
                "chunk_id": chunk_id,
                "status": "completed",
                "start_time": start_time,
                "end_time": end_time,
                "text": transcript.text,
                "processing_time": processing_time
            }
            
        except Exception as e:
            processing_time = time.time() - chunk_start
            logger.error(f"Error transcribing chunk {chunk_id}: {e}")
            return {
                "chunk_id": chunk_id,
                "status": "error",
                "start_time": start_time,
                "end_time": end_time,
                "error": str(e),
                "processing_time": processing_time
            }
    
    async def process_single_video(self, audio_file: str, title: str, duration: float) -> dict:
        """Process short video without chunking"""
        logger.info(f"Processing {title} as single unit (duration: {duration:.1f}s)")
        
        start_time = time.time()
        transcriber = aai.Transcriber()
        
        # Upload and transcribe in one go
        transcript = await asyncio.get_event_loop().run_in_executor(
            self.executor,
            lambda: transcriber.transcribe(audio_file)
        )
        
        processing_time = time.time() - start_time
        
        if transcript.status == aai.TranscriptStatus.error:
            raise HTTPException(status_code=500, detail=f"Transcription failed: {transcript.error}")
        
        return {
            "text": transcript.text,
            "status": "completed",
            "video_title": title,
            "audio_duration": duration,  # Changed from total_duration to match frontend
            "total_duration": duration,
            "total_chunks": 1,
            "processing_time": processing_time,
            "service_method": "single_unit",
            "chunks": [{
                "chunk_id": 0,
                "status": "completed",
                "start_time": 0.0,
                "end_time": duration,
                "processing_time": processing_time
            }]
        }

@app.get("/")
async def root():
    return {"message": "Optimized AssemblyAI Chunking Service", "version": "1.0.0"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/test")
async def test():
    """Test endpoint to verify service is working"""
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    return {
        "status": "working",
        "assemblyai_configured": bool(api_key),
        "api_key_length": len(api_key) if api_key else 0
    }

# Updated main transcription endpoint
@app.post("/transcribe-chunked")
async def transcribe_optimized(request: ChunkingRequest):
    """Optimized transcription with smart chunking decisions"""
    job_id = str(uuid.uuid4())
    start_time = time.time()
    
    try:
        logger.info(f"Starting optimized transcription for job {job_id}: {request.url}")
        
        # Check API key
        api_key = os.getenv('ASSEMBLYAI_API_KEY')
        if not api_key:
            raise HTTPException(status_code=500, detail="AssemblyAI API key not configured")
        
        aai.settings.api_key = api_key
        
        # Initialize service
        service = OptimizedChunkingService()
        
        # Download video and extract audio
        video_title, audio_file, duration = get_video_info_and_audio(str(request.url))
        download_time = time.time() - start_time
        
        logger.info(f"Video downloaded in {download_time:.2f}s: {video_title} ({duration:.1f}s)")
        
        # Decide processing strategy
        if not service.should_use_chunking(duration):
            logger.info("Short video detected - processing without chunking")
            result = await service.process_single_video(audio_file, video_title, duration)
            result["download_time"] = download_time
            
            # Cleanup
            try:
                if os.path.exists(audio_file):
                    audio_dir = os.path.dirname(audio_file)
                    shutil.rmtree(audio_dir)
            except:
                pass
            
            return result
        
        # Use chunking for long videos
        logger.info("Long video detected - using optimized chunking")
        
        chunk_creation_start = time.time()
        chunks = service.create_chunks_in_memory(audio_file, request.chunk_size_minutes or CHUNK_SIZE_MINUTES)
        chunk_creation_time = time.time() - chunk_creation_start
        
        # Process chunks with higher concurrency
        semaphore = asyncio.Semaphore(request.max_concurrent or MAX_CONCURRENT_CHUNKS)
        
        async def process_chunk_with_semaphore(chunk_data):
            async with semaphore:
                chunk_bytes, start_t, end_t = chunk_data
                chunk_id = chunks.index(chunk_data)
                return await service.transcribe_chunk_from_memory(
                    chunk_bytes, chunk_id, start_t, end_t
                )
        
        transcription_start = time.time()
        chunk_results = await asyncio.gather(
            *[process_chunk_with_semaphore(chunk_data) for chunk_data in chunks],
            return_exceptions=True
        )
        transcription_time = time.time() - transcription_start
        
        # Process results
        processed_results = []
        for i, result in enumerate(chunk_results):
            if isinstance(result, Exception):
                logger.error(f"Chunk {i} failed: {result}")
                processed_results.append({
                    "chunk_id": i,
                    "status": "error",
                    "start_time": chunks[i][1],
                    "end_time": chunks[i][2],
                    "error": str(result)
                })
            else:
                processed_results.append(result)
        
        # Merge transcripts
        merge_start = time.time()
        successful_chunks = [r for r in processed_results if r["status"] == "completed"]
        successful_chunks.sort(key=lambda x: x["start_time"])
        
        merged_text = " ".join([chunk["text"] for chunk in successful_chunks if chunk.get("text")])
        merge_time = time.time() - merge_start
        
        total_processing_time = time.time() - start_time
        
        # Cleanup
        try:
            if os.path.exists(audio_file):
                audio_dir = os.path.dirname(audio_file)
                shutil.rmtree(audio_dir)
        except:
            pass
        
        logger.info(f"Chunked processing completed in {total_processing_time:.2f}s")
        logger.info(f"Timing breakdown - Download: {download_time:.2f}s, "
                   f"Chunks: {chunk_creation_time:.2f}s, "
                   f"Transcription: {transcription_time:.2f}s, "
                   f"Merge: {merge_time:.2f}s")
        
        return {
            "text": merged_text,
            "status": "completed",
            "video_title": video_title,
            "audio_duration": duration,  # Added to match frontend expectations
            "total_duration": duration,
            "total_chunks": len(chunks),
            "processing_time": total_processing_time,
            "service_method": "optimized_chunking",
            "timing_breakdown": {
                "download": download_time,
                "chunk_creation": chunk_creation_time,
                "transcription": transcription_time,
                "merge": merge_time
            },
            "chunks": processed_results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Job {job_id} failed with error: {e}")
        logger.error(f"Error type: {type(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        
        # Return a more detailed error response
        raise HTTPException(
            status_code=500, 
            detail=f"Transcription failed: {str(e)} (Type: {type(e).__name__})"
        )

@app.get("/job/{job_id}")
async def get_job_status(job_id: str):
    """Get status of a transcription job"""
    if job_id not in job_progress:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job_progress[job_id]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=8002,
        reload=True,
        log_level="info"
    )