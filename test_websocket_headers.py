import asyncio
import websockets
import json
import os
from dotenv import load_dotenv

load_dotenv()

async def test_header_auth():
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    print(f"🔑 Using API key: {api_key[:10]}...")
    
    uri = "wss://streaming.assemblyai.com/v3/ws"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        print(f"\n🔗 Trying additional_headers...")
        
        async with websockets.connect(uri, additional_headers=headers) as websocket:
            print("✅ WebSocket connected with additional_headers!")
            
            # Send session start
            config = {
                "type": "session_start",
                "sample_rate": 16000,
                "format_turns": True
            }
            
            await websocket.send(json.dumps(config))
            print("✅ Session start sent")
            
            # Wait for response
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=5)
                data = json.loads(response)
                print(f"✅ Received: {data}")
                
                if data.get("type") == "session_started":
                    print("�� SUCCESS with additional_headers!")
                    return True
                else:
                    print(f"⚠️ Unexpected response: {data}")
                    
            except asyncio.TimeoutError:
                print("⚠️ No response received within 5 seconds")
                
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    
    return False

if __name__ == "__main__":
    success = asyncio.run(test_header_auth())
    if success:
        print(f"\n🎯 Use additional_headers in your main.py")
    else:
        print("\n❌ additional_headers didn't work")
