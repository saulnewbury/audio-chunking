import os
import asyncio
import aiohttp
from dotenv import load_dotenv

load_dotenv()

async def test_openai_setup():
    """Test OpenAI API setup"""
    
    api_key = os.getenv('OPENAI_API_KEY')
    
    print("🔑 Testing OpenAI API Setup")
    print(f"API Key present: {bool(api_key)}")
    
    if not api_key:
        print("❌ OPENAI_API_KEY not found in environment")
        print("Please add to .env file: OPENAI_API_KEY=your_key_here")
        return False
    
    print(f"API Key preview: {api_key[:10]}...{api_key[-4:]}")
    
    # Test API connection
    url = "https://api.openai.com/v1/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    models = [m['id'] for m in data['data'] if 'whisper' in m['id']]
                    print(f"✅ API connection successful")
                    print(f"🎤 Available Whisper models: {models}")
                    return True
                else:
                    error_text = await response.text()
                    print(f"❌ API error {response.status}: {error_text}")
                    return False
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_openai_setup())
    
    if success:
        print("\n✅ OpenAI setup is working!")
        print("🚀 Ready to test Whisper transcription")
    else:
        print("\n❌ OpenAI setup needs fixing")
        print("💡 Common issues:")
        print("   - Missing or incorrect API key")
        print("   - Insufficient credits/quota")
        print("   - Network connectivity issues")