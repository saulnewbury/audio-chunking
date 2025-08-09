import asyncio
import websockets
import json

async def test_main_service():
    """Test the main service with Steve Jobs video"""
    
    uri = "ws://localhost:8002/stream-transcribe"
    
    print("🧪 Testing main service with Steve Jobs video...")
    
    try:
        async with websockets.connect(uri) as websocket:
            request = {
                "video_url": "https://www.youtube.com/watch?v=D1R-jKKp3NA",
                "chunk_duration_ms": 50,
                "enable_formatting": True
            }
            
            await websocket.send(json.dumps(request))
            
            transcript_count = 0
            
            async for message in websocket:
                data = json.loads(message)
                message_type = data.get("type")
                
                if message_type == "video_info":
                    print(f"📹 {data['title']}")
                    
                elif message_type == "connection_established":
                    print("🔗 Connected to Universal-Streaming")
                    
                elif message_type == "progress":
                    duration = data.get("duration_processed", 0)
                    if int(duration) % 10 == 0:  # Every 10 seconds
                        print(f"📤 Processing: {duration:.0f}s")
                        
                elif message_type == "transcript":
                    text = data.get("text", "")
                    transcript_count += 1
                    print(f"🎉 TRANSCRIPT {transcript_count}: '{text}'")
                    
                elif message_type == "complete":
                    total_transcripts = data.get("total_transcripts", 0)
                    final_text = data.get("final_text", "")
                    print(f"\n✅ COMPLETE!")
                    print(f"   Total transcripts: {total_transcripts}")
                    print(f"   Final text: {final_text[:200]}...")
                    break
                    
                elif message_type == "error":
                    print(f"❌ Error: {data.get('message')}")
                    break
                    
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    print("🚀 Testing main streaming service")
    print("🎯 Make sure main.py is running on port 8002")
    asyncio.run(test_main_service())