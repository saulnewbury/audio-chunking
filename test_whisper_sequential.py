import asyncio
import websockets
import json

async def test_whisper_sequential():
    """Test Whisper with sequential processing (no concurrency)"""
    
    uri = "ws://localhost:8005/whisper-fast"
    
    print("🔬 Testing Whisper Sequential Processing")
    print("🎯 Process chunks one by one to isolate concurrency issues")
    
    try:
        async with websockets.connect(uri) as websocket:
            request = {
                "video_url": "https://www.youtube.com/watch?v=jNQXAC9IVRw",  # Short 19s video
                "chunk_duration_seconds": 60.0,
                "max_concurrent_chunks": 1  # Process ONE chunk at a time
            }
            
            await websocket.send(json.dumps(request))
            
            async for message in websocket:
                data = json.loads(message)
                message_type = data.get("type")
                
                if message_type == "video_info":
                    title = data.get("title")
                    duration = data.get("duration")
                    print(f"📹 Video: {title} ({duration}s)")
                    
                elif message_type == "chunks_created":
                    total_chunks = data.get("total_chunks")
                    print(f"📦 Created {total_chunks} chunks (processing sequentially)")
                    
                elif message_type == "chunk_complete":
                    chunk_id = data.get("chunk_id")
                    text = data.get("text", "")
                    processing_time = data.get("processing_time")
                    print(f"✅ Chunk {chunk_id}: '{text[:60]}...' ({processing_time:.2f}s)")
                    
                elif message_type == "progress":
                    completed = data.get("completed")
                    failed = data.get("failed")
                    total = data.get("total")
                    print(f"📊 Progress: {completed} success, {failed} failed out of {total}")
                    
                elif message_type == "transcription_complete":
                    successful = data.get("successful_chunks")
                    failed_chunks = data.get("failed_chunks")
                    full_transcript = data.get("full_transcript", "")
                    
                    print(f"\n🎉 SEQUENTIAL TEST COMPLETE!")
                    print(f"   Successful: {successful}")
                    print(f"   Failed: {failed_chunks}")
                    print(f"   Transcript: {full_transcript[:100]}...")
                    
                    if successful > 0:
                        print("✅ Sequential processing works - concurrency is the issue")
                    else:
                        print("❌ Even sequential fails - basic API call problem")
                    
                elif message_type == "performance":
                    break
                    
                elif message_type == "error":
                    error_msg = data.get("message")
                    print(f"❌ Error: {error_msg}")
                    break
                    
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_whisper_sequential())