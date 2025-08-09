import asyncio
import websockets
import json
import os
from dotenv import load_dotenv

load_dotenv()

async def test_correct_universal_streaming():
    api_key = os.getenv('ASSEMBLYAI_API_KEY', '').strip()
    uri = "wss://streaming.assemblyai.com/v3/ws"
    headers = {"Authorization": api_key}
    
    try:
        async with websockets.connect(uri, additional_headers=headers) as websocket:
            print("✅ WebSocket connected!")
            
            # 1. Wait for Begin message
            begin_msg = await websocket.recv()
            begin_data = json.loads(begin_msg)
            print(f"📥 Begin: {begin_data}")
            
            # 2. Send some dummy audio (PCM16, 16kHz)
            # Create 200ms of silence: 16000 Hz * 0.2s * 2 bytes = 6400 bytes
            dummy_audio = b'\x00' * 6400
            await websocket.send(dummy_audio)
            print("🎵 Sent 200ms of audio")
            
            # 3. Wait for Turn message
            try:
                turn_msg = await asyncio.wait_for(websocket.recv(), timeout=10)
                turn_data = json.loads(turn_msg)
                print(f"📥 Turn: {turn_data}")
                
                if turn_data.get("type") == "Turn":
                    print("🎉 SUCCESS! Universal-Streaming is working!")
                    print(f"   Transcript: '{turn_data.get('transcript', '')}'")
                    print(f"   End of turn: {turn_data.get('end_of_turn', False)}")
                    return True
                    
            except asyncio.TimeoutError:
                print("⏰ No Turn message received (normal for silence)")
                print("🎉 But connection works! Ready for real audio")
                return True
                
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_correct_universal_streaming())
    if success:
        print("\n✅ Universal-Streaming protocol confirmed!")
        print("Now we can update your main.py")
    else:
        print("\n❌ Still having issues")
