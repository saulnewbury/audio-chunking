import asyncio
import websockets
import json
import os
from dotenv import load_dotenv

load_dotenv()

async def test_auth_formats():
    api_key = os.getenv('ASSEMBLYAI_API_KEY', '').strip()
    uri = "wss://streaming.assemblyai.com/v3/ws"
    
    auth_formats = [
        {"Authorization": f"Bearer {api_key}"},
        {"authorization": f"Bearer {api_key}"},
        {"Authorization": api_key},
        {"authorization": api_key},
    ]
    
    for i, headers in enumerate(auth_formats):
        print(f"\n🧪 Test {i+1}: {list(headers.keys())[0]}")
        
        try:
            async with websockets.connect(uri, additional_headers=headers) as websocket:
                print("✅ WebSocket connected!")
                
                config = {"type": "session_start"}
                await websocket.send(json.dumps(config))
                
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=3)
                    data = json.loads(response)
                    print(f"✅ Response: {data}")
                    
                    if data.get("type") == "session_started":
                        print(f"🎉 SUCCESS with format {i+1}!")
                        return headers
                        
                except asyncio.TimeoutError:
                    print("⚠️ No response")
                    
        except Exception as e:
            print(f"❌ Failed: {e}")
    
    return None

if __name__ == "__main__":
    result = asyncio.run(test_auth_formats())
    if result:
        print(f"\n🎯 Working auth format: {result}")
    else:
        print("\n❌ No auth format worked")
