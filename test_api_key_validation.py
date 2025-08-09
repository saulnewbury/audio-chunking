import requests
import os
from dotenv import load_dotenv

load_dotenv()

def test_api_key():
    api_key = os.getenv('ASSEMBLYAI_API_KEY', '').strip()
    
    print(f"🔑 Testing API key: {api_key[:10]}...")
    
    # Test with AssemblyAI's regular API (lowercase authorization)
    headers = {
        "authorization": api_key,
        "content-type": "application/json"
    }
    
    try:
        response = requests.get(
            "https://api.assemblyai.com/v2/transcript", 
            headers=headers,
            timeout=10
        )
        
        print(f"📡 Regular API status: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ API key is valid for regular API")
            data = response.json()
            if isinstance(data, dict) and 'transcripts' in data:
                print(f"   Transcripts found: {len(data.get('transcripts', []))}")
        elif response.status_code == 401:
            print("❌ API key is invalid or expired")
        else:
            print(f"⚠️ Unexpected status: {response.status_code}")
            
    except Exception as e:
        print(f"❌ API test failed: {e}")

if __name__ == "__main__":
    test_api_key()
