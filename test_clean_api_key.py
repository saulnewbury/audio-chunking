import asyncio
import websockets
import json
import os
from dotenv import load_dotenv

load_dotenv()

async def test_clean_connection():
    api_key = os.getenv('ASSEMBLYAI_API_KEY', '').strip()  # Clean whitespace
    print(f"🔑 Cleaned API key: {api_key[:10]}... (length: {len(api_key)})")
    
    uri = "wss://streaming.assemblyai.com/v3/ws"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    print(f"📋 Authorization header: Bearer {api_key[:10]}...")
    
    try:
        async with websockets.connect(uri, additional_headers=headers) as websocket:
            print("✅ WebSocket connected!")
            
            # Send minimal session start
            config = {"type": "session_start"}
            config_json = json.dumps(config)
            
            print(f"📤 Sending: {config_json}")
            await websocket.send(config_json)
            
            # Wait for response
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=10)
                print(f"📥 Raw response: {response}")
                
                try:
                    data = json.loads(response)
                    print(f"📋 Parsed response: {data}")
                    
                    if data.get("type") == "session_started":
                        print("🎉 SUCCESS!")
                        return True
                    elif data.get("type") == "error":
                        print(f"❌ API Error: {data.get('message', 'Unknown error')}")
                    else:
                        print(f"⚠️ Unexpected response type: {data.get('type')}")
                        
                except json.JSONDecodeError:
                    print(f"⚠️ Response not JSON: {response}")
                    
            except asyncio.TimeoutError:
                print("⚠️ No response within 10 seconds")
                
    except Exception as e:
        print(f"❌ Connection error: {e}")
        print(f"Error type: {type(e)}")
        return False
    
    return False

if __name__ == "__main__":
    asyncio.run(test_clean_connection())
