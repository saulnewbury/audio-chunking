import asyncio
import websockets
import json
import os
from dotenv import load_dotenv

load_dotenv()

async def test_simple_connection():
    api_key = os.getenv('ASSEMBLYAI_API_KEY')
    print(f"🔑 Using API key: {api_key[:10]}...")
    
    # Try URL-based authentication (should work with older websockets)
    uri = f"wss://streaming.assemblyai.com/v3/ws?api_key={api_key}"
    
    try:
        print(f"🔗 Connecting to: wss://streaming.assemblyai.com/v3/ws?api_key={api_key[:10]}...")
        async with websockets.connect(uri) as websocket:
            print("✅ WebSocket connected!")
            
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
                response = await asyncio.wait_for(websocket.recv(), timeout=10)
                data = json.loads(response)
                print(f"✅ Received: {data}")
                
                if data.get("type") == "session_started":
                    print("🎉 Universal-Streaming session started!")
                    
                    # Send a test message to end session
                    await websocket.send(json.dumps({"type": "session_end"}))
                    print("✅ Session ended cleanly")
                else:
                    print(f"⚠️ Unexpected response type: {data.get('type')}")
                    
            except asyncio.TimeoutError:
                print("⚠️ No response received within 10 seconds")
            except Exception as recv_error:
                print(f"⚠️ Error receiving response: {recv_error}")
                
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("🔍 This might be an API key issue or network problem")

if __name__ == "__main__":
    asyncio.run(test_simple_connection())
