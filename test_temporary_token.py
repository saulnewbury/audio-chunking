import asyncio
import websockets
import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()

async def test_temporary_token_auth():
    api_key = os.getenv('ASSEMBLYAI_API_KEY', '').strip()
    
    # Step 1: Create temporary token
    print("🔑 Creating temporary token...")
    
    headers = {"authorization": api_key}
    token_data = {
        "expires_in_seconds": 3600,
        "max_session_duration_seconds": 3600
    }
    
    try:
        response = requests.post(
            "https://api.assemblyai.com/v2/streaming/token",
            json=token_data,
            headers=headers
        )
        
        print(f"�� Token creation status: {response.status_code}")
        
        if response.status_code == 200:
            token_response = response.json()
            temp_token = token_response.get("token")
            print(f"✅ Temporary token created: {temp_token[:20]}...")
            
            # Step 2: Use temporary token with WebSocket
            uri = f"wss://streaming.assemblyai.com/v3/ws?token={temp_token}"
            
            try:
                async with websockets.connect(uri) as websocket:
                    print("✅ WebSocket connected with temporary token!")
                    
                    config = {
                        "type": "session_start",
                        "sample_rate": 16000,
                        "format_turns": True
                    }
                    
                    await websocket.send(json.dumps(config))
                    print("✅ Session start sent")
                    
                    response = await asyncio.wait_for(websocket.recv(), timeout=5)
                    data = json.loads(response)
                    print(f"✅ Received: {data}")
                    
                    if data.get("type") == "session_started":
                        print("🎉 SUCCESS with temporary token!")
                        return True
                        
            except Exception as ws_error:
                print(f"❌ WebSocket error: {ws_error}")
                
        else:
            print(f"❌ Token creation failed: {response.text}")
            
    except Exception as e:
        print(f"❌ Token creation error: {e}")
    
    return False

if __name__ == "__main__":
    asyncio.run(test_temporary_token_auth())
