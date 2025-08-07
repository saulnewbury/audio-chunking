# chunking_service/main.py - Corrected AssemblyAI API
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, HttpUrl
from typing import Optional, List, Dict, Any
import asyncio
import aiohttp
import tempfile
import os
import logging
from datetime import datetime
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
import yt_dlp
from pydub import AudioSegment
import assemblyai as aai
import math

from dotenv import load_dotenv
load_dotenv()  # This will load the .env file

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AssemblyAI Chunking Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://your-domain.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global configuration
MAX_CONCURRENT_REQUESTS = 3  # Conservative for API rate limits
CHUNK_SIZE_MINUTES = 4  # 4-minute chunks
OVERLAP_SECONDS = 1  # 1-second overlap to avoid word cutoffs

class ChunkingRequest(BaseModel):
    url: HttpUrl
    chunk_size_minutes: Optional[int] = CHUNK_SIZE_MINUTES
    max_concurrent: Optional[int] = MAX_CONCURRENT_REQUESTS
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
    chunks: List[ChunkProgress]

# In-memory storage for job progress (use Redis in production)
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
        # Configure yt-dlp for audio extraction
        ydl_opts = {
            'format': 'bestaudio/best',
            'extractaudio': True,
            'audioformat': 'mp3',
            'audioquality': '128',  # Good balance of quality/size
            'outtmpl': tempfile.gettempdir() + '/%(title)s.%(ext)s',
            'noplaylist': True,
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Get video info first
            info = ydl.extract_info(url, download=False)
            title = info.get('title', 'YouTube Video')
            duration = info.get('duration', 0)
            
            # Skip very long videos (> 3 hours) to avoid timeout
            if duration > 10800:
                raise ValueError("Video is too long (>3 hours). Please try a shorter video.")
            
            logger.info(f"Video: {title}, Duration: {duration}s")
            
            # Download audio
            audio_info = ydl.extract_info(url, download=True)
            audio_file = ydl.prepare_filename(audio_info).replace('.webm', '.mp3').replace('.m4a', '.mp3')
            
            return title, audio_file, duration
            
    except Exception as e:
        logger.error(f"Error extracting audio: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to extract audio: {str(e)}")

def create_audio_chunks(audio_file: str, chunk_size_minutes: int, overlap_seconds: int = 1) -> List[tuple[str, float, float]]:
    """Split audio into chunks with overlap"""
    try:
        audio = AudioSegment.from_file(audio_file)
        audio_duration = len(audio) / 1000.0  # Convert to seconds
        
        chunk_size_ms = chunk_size_minutes * 60 * 1000  # Convert to milliseconds
        overlap_ms = overlap_seconds * 1000
        
        chunks = []
        start_ms = 0
        chunk_id = 0
        
        while start_ms < len(audio):
            # Calculate end position
            end_ms = min(start_ms + chunk_size_ms, len(audio))
            
            # Extract chunk with overlap
            chunk_audio = audio[start_ms:end_ms]
            
            # Save chunk to temporary file
            chunk_filename = f"{tempfile.gettempdir()}/chunk_{chunk_id}_{uuid.uuid4().hex[:8]}.mp3"
            chunk_audio.export(chunk_filename, format="mp3")
            
            start_time = start_ms / 1000.0
            end_time = end_ms / 1000.0
            
            chunks.append((chunk_filename, start_time, end_time))
            
            logger.info(f"Created chunk {chunk_id}: {start_time:.1f}s - {end_time:.1f}s ({chunk_filename})")
            
            # Move to next chunk (subtract overlap)
            start_ms = end_ms - overlap_ms
            chunk_id += 1
            
            # Safety check
            if chunk_id > 100:  # Max 100 chunks (~6.5 hours at 4-min chunks)
                logger.warning("Too many chunks, stopping")
                break
        
        logger.info(f"Created {len(chunks)} chunks from {audio_duration:.1f}s audio")
        return chunks
        
    except Exception as e:
        logger.error(f"Error creating chunks: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create audio chunks: {str(e)}")

async def transcribe_chunk_async(chunk_file: str, chunk_id: int, start_time: float, end_time: float) -> ChunkProgress:
    """Transcribe a single chunk asynchronously using correct AssemblyAI API"""
    progress = ChunkProgress(
        chunk_id=chunk_id,
        status="processing",
        start_time=start_time,
        end_time=end_time
    )
    
    chunk_start_time = asyncio.get_event_loop().time()
    
    try:
        logger.info(f"Starting transcription for chunk {chunk_id} ({start_time:.1f}s - {end_time:.1f}s)")
        
        # Create transcriber
        transcriber = aai.Transcriber()
        
        # Upload file and transcribe directly
        transcript = await asyncio.get_event_loop().run_in_executor(
            None, lambda: transcriber.transcribe(chunk_file)
        )
        
        if transcript.status == aai.TranscriptStatus.error:
            progress.status = "error"
            progress.error = transcript.error
            logger.error(f"Chunk {chunk_id} failed: {transcript.error}")
        else:
            progress.status = "completed"
            progress.text = transcript.text
            logger.info(f"Chunk {chunk_id} completed: {len(transcript.text) if transcript.text else 0} characters")
        
        progress.processing_time = asyncio.get_event_loop().time() - chunk_start_time
        
    except Exception as e:
        progress.status = "error"
        progress.error = str(e)
        progress.processing_time = asyncio.get_event_loop().time() - chunk_start_time
        logger.error(f"Error processing chunk {chunk_id}: {e}")
    
    finally:
        # Cleanup chunk file
        try:
            os.remove(chunk_file)
        except:
            pass
    
    return progress

def merge_chunk_transcripts(chunk_results: List[ChunkProgress], overlap_seconds: int = 1) -> str:
    """Merge transcripts from chunks, handling overlaps"""
    if not chunk_results:
        return ""
    
    # Sort by start time
    chunk_results.sort(key=lambda x: x.start_time)
    
    merged_text = []
    
    for i, chunk in enumerate(chunk_results):
        if chunk.status != "completed" or not chunk.text:
            logger.warning(f"Skipping chunk {chunk.chunk_id} - status: {chunk.status}")
            continue
        
        text = chunk.text.strip()
        
        if i == 0:
            # First chunk - use as-is
            merged_text.append(text)
        else:
            # Subsequent chunks - try to remove overlap
            # Simple approach: remove first few words that might overlap
            words = text.split()
            if len(words) > 3:
                # Skip first 3 words to handle overlap
                overlap_removed = " ".join(words[3:])
                merged_text.append(overlap_removed)
            else:
                merged_text.append(text)
    
    return " ".join(merged_text)

@app.get("/")
async def root():
    return {"message": "AssemblyAI Chunking Service", "version": "1.0.0"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/transcribe-chunked")
async def transcribe_chunked(request: ChunkingRequest):
    """Transcribe video using chunking strategy"""
    job_id = str(uuid.uuid4())
    start_time = asyncio.get_event_loop().time()
    
    try:
        # Initialize job progress
        job_progress[job_id] = {
            "status": "downloading",
            "progress": 0,
            "message": "Downloading and extracting audio..."
        }
        
        logger.info(f"Starting chunked transcription for job {job_id}: {request.url}")
        
        # Check if AssemblyAI API key is configured
        api_key = os.getenv('ASSEMBLYAI_API_KEY')
        if not api_key:
            raise HTTPException(status_code=500, detail="AssemblyAI API key not configured")
        
        # Set API key for AssemblyAI
        aai.settings.api_key = api_key
        
        # Step 1: Download video and extract audio
        video_title, audio_file, duration = get_video_info_and_audio(str(request.url))
        
        job_progress[job_id].update({
            "status": "chunking",
            "message": f"Creating audio chunks for {video_title}..."
        })
        
        # Step 2: Create chunks (skip chunking for short videos)
        if duration < 300:  # Less than 5 minutes
            logger.info("Short video detected, skipping chunking")
            chunk_size_minutes = int(duration / 60) + 1  # Single chunk
        else:
            chunk_size_minutes = request.chunk_size_minutes
        
        chunks = create_audio_chunks(audio_file, chunk_size_minutes, OVERLAP_SECONDS)
        
        job_progress[job_id].update({
            "status": "transcribing",
            "message": f"Transcribing {len(chunks)} chunks in parallel...",
            "total_chunks": len(chunks)
        })
        
        # Step 3: Process chunks in parallel with semaphore
        semaphore = asyncio.Semaphore(request.max_concurrent)
        
        async def transcribe_with_semaphore(chunk_data):
            async with semaphore:
                chunk_file, start_time, end_time = chunk_data
                chunk_id = chunks.index(chunk_data)
                return await transcribe_chunk_async(chunk_file, chunk_id, start_time, end_time)
        
        # Process all chunks concurrently
        chunk_results = await asyncio.gather(
            *[transcribe_with_semaphore(chunk_data) for chunk_data in chunks],
            return_exceptions=True
        )
        
        # Handle any exceptions
        processed_results = []
        for i, result in enumerate(chunk_results):
            if isinstance(result, Exception):
                logger.error(f"Chunk {i} failed with exception: {result}")
                processed_results.append(ChunkProgress(
                    chunk_id=i,
                    status="error",
                    start_time=chunks[i][1],
                    end_time=chunks[i][2],
                    error=str(result)
                ))
            else:
                processed_results.append(result)
        
        # Step 4: Merge results
        job_progress[job_id].update({
            "status": "merging",
            "message": "Merging chunk transcripts..."
        })
        
        final_text = merge_chunk_transcripts(processed_results, OVERLAP_SECONDS)
        total_processing_time = asyncio.get_event_loop().time() - start_time
        
        # Cleanup
        try:
            os.remove(audio_file)
        except:
            pass
        
        # Final result
        successful_chunks = [r for r in processed_results if r.status == "completed"]
        failed_chunks = [r for r in processed_results if r.status == "error"]
        
        logger.info(f"Job {job_id} completed: {len(successful_chunks)}/{len(chunks)} chunks successful")
        
        if not successful_chunks:
            raise HTTPException(status_code=500, detail="All chunks failed to process")
        
        job_progress[job_id].update({
            "status": "completed",
            "message": f"Transcription completed in {total_processing_time:.1f}s"
        })
        
        return TranscriptResponse(
            text=final_text,
            status="completed",
            video_title=video_title,
            total_duration=duration,
            total_chunks=len(chunks),
            processing_time=total_processing_time,
            chunks=processed_results
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        job_progress[job_id].update({
            "status": "error",
            "message": str(e)
        })
        raise HTTPException(status_code=500, detail=f"Transcription failed: {str(e)}")

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
        port=8002,  # Different port from transcript service
        reload=True,
        log_level="info"
    )