import os
import base64
import json
from dotenv import load_dotenv

load_dotenv()

def analyze_api_key():
    api_key = os.getenv('ASSEMBLYAI_API_KEY', '').strip()
    
    print(f"🔑 API Key Analysis:")
    print(f"   Full key: {api_key}")
    print(f"   Length: {len(api_key)}")
    
    # Check if it looks like a JWT (contains dots)
    if '.' in api_key:
        print("   Format: Looks like JWT (contains dots)")
        parts = api_key.split('.')
        print(f"   JWT parts: {len(parts)}")
        
        for i, part in enumerate(parts):
            print(f"   Part {i+1}: {part[:20]}... (length: {len(part)})")
            
            # Try to decode each part
            try:
                # Add padding if needed
                padded = part + '=' * (4 - len(part) % 4)
                decoded = base64.urlsafe_b64decode(padded)
                try:
                    json_data = json.loads(decoded)
                    print(f"     Decoded JSON: {json_data}")
                except:
                    print(f"     Decoded bytes: {decoded}")
            except Exception as e:
                print(f"     Decode error: {e}")
    else:
        print("   Format: Regular API key (no dots)")
        
    # Check character set
    allowed_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.')
    key_chars = set(api_key)
    unexpected_chars = key_chars - allowed_chars
    
    if unexpected_chars:
        print(f"   ⚠️ Unexpected characters: {unexpected_chars}")
    else:
        print("   ✅ All characters are valid")

if __name__ == "__main__":
    analyze_api_key()
