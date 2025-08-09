import asyncio
import websockets
import json
import time

async def test_local_whisper():
    """Test local Whisper for maximum speed"""
    
    uri = "ws://localhost:8006/local-whisper"
    
    print("🚀 Testing Local Whisper Ultra-Fast Transcription")
    print("🎯 TARGET: 5 seconds for 14.6-minute video")
    print("⚡ Method: Local Whisper with multiprocessing")
    print("💰 Cost: FREE - No API costs!")
    
    try:
        async with websockets.connect(uri) as websocket:
            request = {
                "video_url": "https://www.youtube.com/watch?v=D1R-jKKp3NA",
                "chunk_duration_seconds": 60.0,   # Larger chunks for local processing
                "max_concurrent_chunks": 4,       # Match CPU cores
                "model_size": "base"               # Fast model
            }
            
            start_time = time.time()
            await websocket.send(json.dumps(request))
            
            chunk_times = []
            
            async for message in websocket:
                data = json.loads(message)
                message_type = data.get("type")
                
                if message_type == "video_info":
                    title = data.get("title")
                    duration = data.get("duration")
                    model_size = data.get("model_size")
                    estimated = data.get("estimated_time")
                    print(f"\n📹 Video: {title}")
                    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
                    print(f"🧠 Model: {model_size}")
                    print(f"🎯 Estimated: {estimated}")
                    
                elif message_type == "chunks_created":
                    total_chunks = data.get("total_chunks")
                    estimated_processing = data.get("estimated_processing")
                    print(f"📦 Created {total_chunks} chunks")
                    print(f"⏱️  Estimated processing: {estimated_processing}")
                    print(f"🚀 Starting local Whisper processing...")
                    
                elif message_type == "chunk_complete":
                    chunk_id = data.get("chunk_id")
                    text = data.get("text", "")
                    processing_time = data.get("processing_time")
                    start_time_chunk = data.get("start_time")
                    
                    chunk_times.append(processing_time)
                    print(f"✅ Chunk {chunk_id} ({start_time_chunk:.0f}s): '{text[:50]}...' ({processing_time:.2f}s)")
                    
                elif message_type == "progress":
                    completed = data.get("completed")
                    failed = data.get("failed")
                    total = data.get("total")
                    percent = data.get("progress_percent")
                    
                    print(f"📊 Progress: {completed}/{total} chunks ({percent:.1f}%) - {failed} failed")
                    
                elif message_type == "transcription_complete":
                    successful = data.get("successful_chunks")
                    failed_chunks = data.get("failed_chunks")
                    avg_chunk_time = data.get("average_chunk_time")
                    full_transcript = data.get("full_transcript", "")
                    
                    print(f"\n🎉 LOCAL WHISPER COMPLETE!")
                    print(f"   Successful: {successful}")
                    print(f"   Failed: {failed_chunks}")
                    print(f"   Average chunk time: {avg_chunk_time:.2f}s")
                    print(f"   Transcript length: {len(full_transcript)} characters")
                    print(f"   First 200 chars: {full_transcript[:200]}...")
                    
                elif message_type == "performance":
                    total_time = data.get("total_time")
                    speedup = data.get("speedup_factor")
                    target_achieved = data.get("target_achieved")
                    
                    print(f"\n🏆 LOCAL WHISPER RESULTS:")
                    print(f"   Total time: {total_time:.2f} seconds")
                    print(f"   Speedup: {speedup:.1f}x faster than real-time")
                    print(f"   5-second target: {'🎯 ACHIEVED!' if target_achieved else '❌ Missed'}")
                    print(f"   Cost: $0.00 (FREE!)")
                    
                    if target_achieved:
                        print(f"\n🚀 SUCCESS! Local Whisper hit the target!")
                        print(f"💡 {speedup:.0f}x faster than real-time processing")
                        print(f"🆓 Zero API costs")
                    else:
                        if chunk_times:
                            fastest = min(chunk_times)
                            avg = sum(chunk_times) / len(chunk_times)
                            print(f"\n🔧 Analysis:")
                            print(f"   Fastest chunk: {fastest:.2f}s")
                            print(f"   Average chunk: {avg:.2f}s")
                            print(f"💡 Try 'tiny' model for speed or more concurrent chunks")
                    
                    break
                    
                elif message_type == "status":
                    message = data.get("message")
                    print(f"📝 {message}")
                    
                elif message_type == "error":
                    error_msg = data.get("message")
                    print(f"❌ Error: {error_msg}")
                    if "Whisper not installed" in error_msg:
                        print("💡 Install with: pip install openai-whisper")
                    break
                    
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    print("🚀 Local Whisper Test")
    print("🎯 Make sure local_whisper_service.py is running on port 8006")
    print("📋 Requires: pip install openai-whisper")
    print()
    asyncio.run(test_local_whisper())