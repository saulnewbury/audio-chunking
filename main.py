# Optimized chunking service with VAD-based chunking
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, HttpUrl
from typing import Optional, List, Dict, Any, Tuple
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
import torch
import torchaudio
import numpy as np

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Optimized AssemblyAI VAD Chunking Service", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://your-domain.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Performance-focused configuration
CHUNK_THRESHOLD_MINUTES = 5   # Lowered from 15 to test VAD chunking
MAX_CONCURRENT_CHUNKS = 5     # Increased concurrency
MIN_CHUNK_DURATION = 15       # Minimum chunk duration in seconds (lowered from 30)
MAX_CHUNK_DURATION = 180      # Maximum chunk duration in seconds (lowered from 300)
VAD_SAMPLE_RATE = 16000       # Silero VAD requires 16kHz

class ChunkingRequest(BaseModel):
    url: HttpUrl
    max_concurrent: Optional[int] = MAX_CONCURRENT_CHUNKS
    language_code: Optional[str] = "en_us"
    use_vad_chunking: Optional[bool] = True
    min_chunk_duration: Optional[int] = MIN_CHUNK_DURATION
    max_chunk_duration: Optional[int] = MAX_CHUNK_DURATION
    # New VAD parameters for fine control
    min_pause_duration: Optional[float] = 1.0  # Minimum pause to consider for splitting (seconds)
    force_split_interval: Optional[int] = 120  # Force split every N seconds if no natural pause

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

# Global VAD model (loaded once)
vad_model = None
vad_utils = None

def load_vad_model():
    """Load Silero VAD model once at startup"""
    global vad_model, vad_utils
    if vad_model is None:
        try:
            logger.info("Loading Silero VAD model...")
            vad_model, vad_utils = torch.hub.load(
                repo_or_dir='snakers4/silero-vad',
                model='silero_vad',
                force_reload=False,
                onnx=False
            )
            logger.info("VAD model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load VAD model: {e}")
            raise

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
                'preferredcodec': 'wav',  # Changed to WAV for better VAD compatibility
                'preferredquality': '192',
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
                if file.startswith(safe_filename) and file.endswith('.wav'):
                    audio_file = os.path.join(temp_dir, file)
                    break
            
            if not audio_file:
                for file in os.listdir(temp_dir):
                    if any(file.endswith(ext) for ext in ['.wav', '.mp3', '.m4a', '.webm']):
                        audio_file = os.path.join(temp_dir, file)
                        if not audio_file.endswith('.wav'):
                            import subprocess
                            wav_file = audio_file.replace(os.path.splitext(audio_file)[1], '.wav')
                            subprocess.run([
                                'ffmpeg', '-i', audio_file, '-acodec', 'pcm_s16le', '-ar', '16000', wav_file
                            ], check=True, capture_output=True)
                            os.remove(audio_file)
                            audio_file = wav_file
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

class VADChunkingService:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CHUNKS)
        # Load VAD model on initialization
        load_vad_model()
    
    def should_use_chunking(self, duration_seconds: float) -> bool:
        """Determine if chunking is beneficial for this video length"""
        return duration_seconds > (CHUNK_THRESHOLD_MINUTES * 60)
    
    def detect_voice_segments(self, audio_file: str, min_duration: int, max_duration: int, min_pause: float = 1.0) -> List[Tuple[float, float]]:
        """
        Use Silero VAD to detect voice segments in audio file
        Returns list of (start_time, end_time) tuples in seconds
        """
        try:
            logger.info("Running VAD analysis...")
            
            # Try multiple backends for torchaudio compatibility
            try:
                # First try with soundfile backend
                import soundfile
                wav, original_sr = torchaudio.load(audio_file, backend="soundfile")
            except:
                try:
                    # Fallback to ffmpeg backend
                    wav, original_sr = torchaudio.load(audio_file, backend="ffmpeg")
                except:
                    # Final fallback - use pydub to convert then load
                    logger.info("Using pydub fallback for audio loading")
                    audio_segment = AudioSegment.from_file(audio_file)
                    # Convert to wav in memory
                    wav_io = io.BytesIO()
                    audio_segment.export(wav_io, format="wav")
                    wav_io.seek(0)
                    wav, original_sr = torchaudio.load(wav_io, backend="soundfile")
            
            logger.info(f"Loaded audio: {wav.shape}, sample rate: {original_sr}")
            
            # Resample to 16kHz if necessary
            if original_sr != VAD_SAMPLE_RATE:
                resampler = torchaudio.transforms.Resample(original_sr, VAD_SAMPLE_RATE)
                wav = resampler(wav)
            
            # Convert to mono if stereo
            if wav.shape[0] > 1:
                wav = torch.mean(wav, dim=0, keepdim=True)
            
            # Squeeze to 1D
            wav = wav.squeeze()
            
            # Get speech timestamps using VAD
            speech_timestamps = vad_utils[0](wav, vad_model, sampling_rate=VAD_SAMPLE_RATE)
            
            logger.info(f"VAD found {len(speech_timestamps)} raw speech segments")
            
            if not speech_timestamps:
                logger.warning("No speech detected in audio")
                # Return single segment covering whole audio as fallback
                audio_duration = len(wav) / VAD_SAMPLE_RATE
                return [(0.0, audio_duration)]
            
            # Convert VAD timestamps to seconds and merge nearby segments
            segments = []
            for timestamp in speech_timestamps:
                start = timestamp['start'] / VAD_SAMPLE_RATE
                end = timestamp['end'] / VAD_SAMPLE_RATE
                segments.append((start, end))
            
            logger.info(f"Raw segments before merging: {len(segments)}")
            for i, (start, end) in enumerate(segments[:5]):  # Show first 5
                logger.info(f"Raw segment {i}: {start:.1f}s - {end:.1f}s")
            
            # Merge segments that are close together and enforce duration limits
            merged_segments = self._merge_segments(segments, min_duration, max_duration, min_pause)
            
            logger.info(f"VAD detected {len(merged_segments)} speech segments after merging")
            for i, (start, end) in enumerate(merged_segments):
                logger.info(f"Final segment {i}: {start:.1f}s - {end:.1f}s (duration: {end-start:.1f}s)")
            
            return merged_segments
            
        except Exception as e:
            logger.error(f"VAD analysis failed: {e}")
            logger.error(f"Error details: {str(e)}")
            # Fallback to single segment
            audio = AudioSegment.from_file(audio_file)
            duration = len(audio) / 1000.0
            logger.info("Falling back to single segment processing")
            return [(0.0, duration)]
    
    def _merge_segments(self, segments: List[Tuple[float, float]], min_duration: int, max_duration: int, min_pause: float = 1.0) -> List[Tuple[float, float]]:
        """
        Merge speech segments to create optimal chunks for transcription with custom pause detection
        """
        if not segments:
            return []
        
        logger.info(f"Merging segments with min_duration={min_duration}s, max_duration={max_duration}s, min_pause={min_pause}s")
        
        merged = []
        current_start, current_end = segments[0]
        
        for start, end in segments[1:]:
            gap = start - current_end
            new_duration = end - current_start
            
            logger.debug(f"Considering merge: gap={gap:.1f}s, new_duration={new_duration:.1f}s")
            
            # Only merge if gap is smaller than min_pause threshold AND won't exceed max duration
            if gap < min_pause and new_duration <= max_duration:
                logger.debug(f"Merging segments: extending to {end:.1f}s")
                current_end = end
            else:
                # Split here - either natural pause found or duration limit reached
                segment_duration = current_end - current_start
                logger.info(f"Splitting at gap={gap:.1f}s: {current_start:.1f}s - {current_end:.1f}s (duration: {segment_duration:.1f}s)")
                
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        
        # Add final segment
        merged.append((current_start, current_end))
        logger.info(f"Added final segment: {current_start:.1f}s - {current_end:.1f}s")
        
        # Apply forced splitting if segments are still too long
        merged = self._apply_forced_splitting(merged, max_duration, min_pause)
        
        logger.info(f"Final merged segments: {len(merged)}")
        return merged
    
    def _apply_forced_splitting(self, segments: List[Tuple[float, float]], max_duration: int, min_pause: float) -> List[Tuple[float, float]]:
        """
        Apply forced splitting to segments that are still too long, finding the best pause within intervals
        """
        final_segments = []
        
        for start, end in segments:
            duration = end - start
            if duration <= max_duration:
                final_segments.append((start, end))
                continue
            
            logger.info(f"Applying forced splitting to segment {start:.1f}s - {end:.1f}s (duration: {duration:.1f}s)")
            
            # Split long segment into smaller chunks at natural pauses if possible
            current_start = start
            
            while current_start < end:
                target_end = min(current_start + max_duration, end)
                
                # If this would be the final small chunk, just extend to the end
                min_duration_threshold = 30  # Don't create tiny trailing chunks
                if end - target_end < min_duration_threshold:
                    target_end = end
                
                final_segments.append((current_start, target_end))
                logger.info(f"Forced split chunk: {current_start:.1f}s - {target_end:.1f}s")
                
                current_start = target_end
        
        return final_segments
    
    def create_vad_chunks(self, audio_file: str, min_duration: int, max_duration: int, min_pause: float = 1.0) -> List[Tuple[bytes, float, float]]:
        """
        Create audio chunks based on VAD speech segments with custom pause detection
        """
        start_time = time.time()
        
        try:
            # Get speech segments using VAD with custom parameters
            segments = self.detect_voice_segments(audio_file, min_duration, max_duration, min_pause)
            
            # Load audio once
            audio = AudioSegment.from_file(audio_file)
            
            chunks = []
            for i, (start_sec, end_sec) in enumerate(segments):
                # Extract audio segment
                start_ms = int(start_sec * 1000)
                end_ms = int(end_sec * 1000)
                chunk_audio = audio[start_ms:end_ms]
                
                # Export to bytes buffer
                buffer = io.BytesIO()
                chunk_audio.export(buffer, format="wav")
                chunk_bytes = buffer.getvalue()
                
                chunks.append((chunk_bytes, start_sec, end_sec))
                
                logger.info(f"Created VAD chunk {i}: {start_sec:.1f}s - {end_sec:.1f}s ({len(chunk_bytes)} bytes)")
            
            processing_time = time.time() - start_time
            logger.info(f"Created {len(chunks)} VAD chunks in {processing_time:.2f}s")
            
            return chunks
            
        except Exception as e:
            logger.error(f"Error creating VAD chunks: {e}")
            raise
    
    async def transcribe_chunk_from_memory(self, chunk_data: bytes, chunk_id: int, 
                                         start_time: float, end_time: float) -> dict:
        """Transcribe chunk directly from memory"""
        chunk_start = time.time()
        
        try:
            logger.info(f"Transcribing VAD chunk {chunk_id} ({start_time:.1f}s - {end_time:.1f}s)")
            
            # Create transcriber
            transcriber = aai.Transcriber()
            
            # Upload chunk bytes directly
            upload_url = await asyncio.get_event_loop().run_in_executor(
                self.executor, 
                lambda: transcriber.upload_file(chunk_data)
            )
            
            # Transcribe
            transcript = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                lambda: transcriber.transcribe(upload_url)
            )
            
            processing_time = time.time() - chunk_start
            
            if transcript.status == aai.TranscriptStatus.error:
                logger.error(f"VAD chunk {chunk_id} failed: {transcript.error}")
                return {
                    "chunk_id": chunk_id,
                    "status": "error",
                    "start_time": start_time,
                    "end_time": end_time,
                    "error": transcript.error,
                    "processing_time": processing_time
                }
            
            logger.info(f"VAD chunk {chunk_id} completed in {processing_time:.2f}s")
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
            logger.error(f"Error transcribing VAD chunk {chunk_id}: {e}")
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
            "audio_duration": duration,
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
    return {"message": "Optimized AssemblyAI VAD Chunking Service", "version": "2.0.0"}

@app.get("/health")
async def health():
    return {"status": "healthy", "vad_loaded": vad_model is not None}

@app.get("/test")
async def test():
    """Test endpoint to verify service is working"""
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    return {
        "status": "working",
        "assemblyai_configured": bool(api_key),
        "api_key_length": len(api_key) if api_key else 0,
        "vad_model_loaded": vad_model is not None
    }

@app.post("/transcribe-chunked")
async def transcribe_optimized(request: ChunkingRequest):
    """Optimized transcription with VAD-based chunking"""
    job_id = str(uuid.uuid4())
    start_time = time.time()
    
    try:
        logger.info(f"Starting VAD-optimized transcription for job {job_id}: {request.url}")
        
        # Check API key
        api_key = os.getenv('ASSEMBLYAI_API_KEY')
        if not api_key:
            raise HTTPException(status_code=500, detail="AssemblyAI API key not configured")
        
        aai.settings.api_key = api_key
        
        # Initialize service
        service = VADChunkingService()
        
        # Download video and extract audio
        video_title, audio_file, duration = get_video_info_and_audio(str(request.url))
        download_time = time.time() - start_time
        
        logger.info(f"Video downloaded in {download_time:.2f}s: {video_title} ({duration:.1f}s)")
        
        # Decide processing strategy
        if not service.should_use_chunking(duration) or not request.use_vad_chunking:
            logger.info("Short video or VAD disabled - processing without chunking")
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
        
        # Use VAD chunking for long videos
        logger.info("Long video detected - using VAD-based chunking")
        
        chunk_creation_start = time.time()
        chunks = service.create_vad_chunks(
            audio_file, 
            request.min_chunk_duration or MIN_CHUNK_DURATION,
            request.max_chunk_duration or MAX_CHUNK_DURATION,
            request.min_pause_duration or 1.0
        )
        chunk_creation_time = time.time() - chunk_creation_start
        
        # Process chunks with concurrency
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
                logger.error(f"VAD chunk {i} failed: {result}")
                processed_results.append({
                    "chunk_id": i,
                    "status": "error",
                    "start_time": chunks[i][1],
                    "end_time": chunks[i][2],
                    "error": str(result)
                })
            else:
                processed_results.append(result)
        
        # Merge transcripts in chronological order
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
        
        logger.info(f"VAD chunked processing completed in {total_processing_time:.2f}s")
        logger.info(f"Timing breakdown - Download: {download_time:.2f}s, "
                   f"VAD+Chunks: {chunk_creation_time:.2f}s, "
                   f"Transcription: {transcription_time:.2f}s, "
                   f"Merge: {merge_time:.2f}s")
        
        return {
            "text": merged_text,
            "status": "completed",
            "video_title": video_title,
            "audio_duration": duration,
            "total_duration": duration,
            "total_chunks": len(chunks),
            "processing_time": total_processing_time,
            "service_method": "vad_chunking",
            "timing_breakdown": {
                "download": download_time,
                "vad_chunk_creation": chunk_creation_time,
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