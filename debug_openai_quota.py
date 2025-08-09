import os
import asyncio
import aiohttp
from dotenv import load_dotenv

load_dotenv()

async def debug_openai_quota():
    """Debug OpenAI quota and billing issues"""
    
    api_key = os.getenv('OPENAI_API_KEY')
    
    print("🔍 Debugging OpenAI Quota Issue")
    print(f"API Key: {api_key[:15]}...{api_key[-6:]}")
    
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        async with aiohttp.ClientSession() as session:
            
            # 1. Check account/organization info
            print("\n1️⃣ Checking account info...")
            async with session.get("https://api.openai.com/v1/organizations", headers=headers) as response:
                if response.status == 200:
                    orgs = await response.json()
                    print(f"✅ Organizations: {len(orgs.get('data', []))}")
                    for org in orgs.get('data', []):
                        print(f"   - {org.get('title', 'Unknown')} ({org.get('id')})")
                else:
                    error = await response.text()
                    print(f"❌ Orgs error {response.status}: {error}")
            
            # 2. Check models access
            print("\n2️⃣ Checking models access...")
            async with session.get("https://api.openai.com/v1/models", headers=headers) as response:
                if response.status == 200:
                    models = await response.json()
                    whisper_models = [m['id'] for m in models['data'] if 'whisper' in m['id']]
                    print(f"✅ Total models: {len(models['data'])}")
                    print(f"✅ Whisper models: {whisper_models}")
                else:
                    error = await response.text()
                    print(f"❌ Models error {response.status}: {error}")
            
            # 3. Try a simple completion (cheaper than Whisper)
            print("\n3️⃣ Testing simple API call...")
            completion_data = {
                "model": "gpt-3.5-turbo",
                "messages": [{"role": "user", "content": "Say 'test'"}],
                "max_tokens": 5
            }
            async with session.post("https://api.openai.com/v1/chat/completions", 
                                   headers={**headers, "Content-Type": "application/json"}, 
                                   json=completion_data) as response:
                result = await response.text()
                print(f"📡 Completion status: {response.status}")
                if response.status == 200:
                    print("✅ Basic API calls work!")
                else:
                    print(f"❌ Completion error: {result}")
            
            # 4. Check usage/billing (might not be accessible)
            print("\n4️⃣ Checking usage...")
            async with session.get("https://api.openai.com/v1/usage", headers=headers) as response:
                if response.status == 200:
                    usage = await response.json()
                    print(f"✅ Usage data: {usage}")
                else:
                    error = await response.text()
                    print(f"ℹ️ Usage endpoint not accessible: {response.status}")
            
            # 5. Test Whisper with a tiny file
            print("\n5️⃣ Testing Whisper with minimal audio...")
            
            # Create a tiny test audio file (1 second of silence)
            import tempfile
            import subprocess
            
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_audio:
                # Create 1 second of silence using ffmpeg
                cmd = [
                    'ffmpeg', '-f', 'lavfi', '-i', 'anullsrc=duration=1:sample_rate=16000',
                    '-y', temp_audio.name
                ]
                try:
                    subprocess.run(cmd, capture_output=True, check=True)
                    
                    # Try to transcribe this tiny file
                    data = aiohttp.FormData()
                    data.add_field('model', 'whisper-1')
                    data.add_field('response_format', 'text')
                    
                    with open(temp_audio.name, 'rb') as f:
                        audio_data = f.read()
                        data.add_field('file', audio_data, filename='test.wav', content_type='audio/wav')
                    
                    async with session.post("https://api.openai.com/v1/audio/transcriptions", 
                                           headers=headers, data=data) as response:
                        result = await response.text()
                        print(f"📡 Whisper status: {response.status}")
                        if response.status == 200:
                            print("✅ Whisper API works!")
                            print(f"📝 Result: '{result}'")
                        else:
                            print(f"❌ Whisper error: {result}")
                    
                    os.unlink(temp_audio.name)
                    
                except subprocess.CalledProcessError:
                    print("⚠️ Could not create test audio file (ffmpeg not available)")
                except Exception as e:
                    print(f"⚠️ Whisper test failed: {e}")
    
    except Exception as e:
        print(f"❌ Debug failed: {e}")

if __name__ == "__main__":
    asyncio.run(debug_openai_quota())