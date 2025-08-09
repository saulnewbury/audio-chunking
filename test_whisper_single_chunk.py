import asyncio
import aiohttp
import os
import tempfile
import yt_dlp
from pydub import AudioSegment
from dotenv import load_dotenv

load_dotenv()

async def test_single_whisper_chunk():
    """Test transcribing a single chunk to isolate the issue"""
    
    print("🔬 Testing single Whisper chunk transcription")
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("❌ OPENAI_API_KEY not found")
        return
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Download a short test video
        print("📥 Downloading test video...")
        video_url = "https://www.youtube.com/watch?v=jNQXAC9IVRw"  # "Me at the zoo" - 19s
        
        ydl_opts = {
            'format': 'bestaudio[ext=m4a]/bestaudio/best',
            'outtmpl': os.path.join(temp_dir, 'test_audio.%(ext)s'),
            'quiet': True,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        
        # Find the audio file
        audio_file = None
        for file in os.listdir(temp_dir):
            if file.endswith('.mp3'):
                audio_file = os.path.join(temp_dir, file)
                break
        
        if not audio_file:
            print("❌ No audio file found")
            return
        
        print(f"🎵 Audio file: {audio_file}")
        print(f"📊 File size: {os.path.getsize(audio_file)} bytes")
        
        # Test transcription
        print("🎤 Testing Whisper transcription...")
        
        url = "https://api.openai.com/v1/audio/transcriptions"
        headers = {"Authorization": f"Bearer {api_key}"}
        
        # Read file into memory first
        with open(audio_file, 'rb') as f:
            audio_data = f.read()
        
        data = aiohttp.FormData()
        data.add_field('model', 'whisper-1')
        data.add_field('response_format', 'text')
        data.add_field('file', audio_data, filename=os.path.basename(audio_file), content_type='audio/mpeg')
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=data, timeout=60) as response:
                response_text = await response.text()
                
                print(f"📡 Response status: {response.status}")
                print(f"📝 Response: {response_text[:200]}...")
                
                if response.status == 200:
                    print("✅ SUCCESS! Whisper transcription works")
                    return True
                else:
                    print(f"❌ FAILED: {response_text}")
                    return False
                    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    success = asyncio.run(test_single_whisper_chunk())
    
    if success:
        print("\n✅ Single chunk works - issue is in the service")
    else:
        print("\n❌ Single chunk fails - need to fix the basic API call")