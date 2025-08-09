import asyncio
import websockets
import json
import os
from dotenv import load_dotenv

load_dotenv()

async def test_session_messages():
    api_key = os.getenv('ASSEMBLYAI_API_KEY', '').strip()
    uri = "wss://streaming.assemblyai.com/v3/ws"
    
    # Use the working auth format (Test 3: Authorization without Bearer)
    headers = {"Authorization": api_key}
    
    # Try different session start messages
    messages_to_try = [
        # Current format
        {"type": "session_start"},
        {"type": "session_start", "sample_rate": 16000},
        {"type": "session_start", "sample_rate": 16000, "format_turns": True},
        
        # Alternative formats
        {"session_start": True},
        {"session_start": {"sample_rate": 16000}},
        {"start": True},
        {"begin": True},
        {"configure": {"sample_rate": 16000}},
        
        # Maybe no initial message needed?
        None
    ]
    
    for i, message in enumerate(messages_to_try):
        print(f"\n🧪 Test {i+1}: {message}")
        
        try:
            async with websockets.connect(uri, additional_headers=headers) as websocket:
                print("✅ WebSocket connected!")
                
                # Wait for Begin message first
                begin_msg = await asyncio.wait_for(websocket.recv(), timeout=3)
                begin_data = json.loads(begin_msg)
                print(f"📥 Begin: {begin_data}")
                
                if message is not None:
                    # Send our message
                    await websocket.send(json.dumps(message))
                    print(f"📤 Sent: {message}")
                    
                    # Wait for response
                    try:
                        response = await asyncio.wait_for(websocket.recv(), timeout=5)
                        data = json.loads(response)
                        print(f"📥 Response: {data}")
                        
                        if data.get("type") in ["session_started", "started", "ready"]:
                            print(f"🎉 SUCCESS with message {i+1}!")
                            return message
                        elif data.get("type") == "error":
                            print(f"❌ Error: {data.get('message', 'Unknown')}")
                        else:
                            print(f"⚠️ Unexpected: {data.get('type')}")
                            
                    except asyncio.TimeoutError:
                        print("⏰ No response to our message")
                        # Maybe the Begin message is enough?
                        if message is None:
                            print("🤔 Maybe Begin message is sufficient?")
                            return True
                else:
                    print("🤔 Testing if Begin message alone is sufficient...")
                    await asyncio.sleep(2)  # Wait to see if anything else comes
                    return True
                    
        except Exception as e:
            print(f"❌ Failed: {e}")
    
    return None

if __name__ == "__main__":
    result = asyncio.run(test_session_messages())
    if result:
        print(f"\n🎯 Working approach: {result}")
    else:
        print("\n❌ No message format worked")
