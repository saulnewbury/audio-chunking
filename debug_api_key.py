import os
import base64
import json
from dotenv import load_dotenv

load_dotenv()

def debug_api_key():
    api_key = os.getenv('ASSEMBLYAI_API_KEY', '')
    
    print(f"🔑 API Key Debug:")
    print(f"   Length: {len(api_key)}")
    print(f"   First 10 chars: {api_key[:10]}")
    print(f"   Last 5 chars: {api_key[-5:]}")
    print(f"   Contains spaces: {' ' in api_key}")
    print(f"   Contains newlines: {'\\n' in api_key or '\\r' in api_key}")
    
    # Check if it's a valid base64-like string
    try:
        # Try to decode as base64 (some API keys are base64 encoded)
        decoded = base64.b64decode(api_key + '==')  # Add padding
        print(f"   Base64 decodeable: Yes")
    except:
        print(f"   Base64 decodeable: No (normal for most API keys)")
    
    # Test the authorization header format
    auth_header = f"Bearer {api_key}"
    print(f"   Auth header length: {len(auth_header)}")
    print(f"   Auth header starts with: {auth_header[:20]}...")
    
    # Test JSON encoding
    test_config = {
        "type": "session_start",
        "sample_rate": 16000,
        "format_turns": True
    }
    
    json_str = json.dumps(test_config)
    print(f"   Test JSON: {json_str}")
    print(f"   JSON length: {len(json_str)}")

if __name__ == "__main__":
    debug_api_key()
