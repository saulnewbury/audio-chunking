import asyncio
import websockets
import json
import time

async def test_optimized_original():
    """Test optimized version of the original working approach"""
    
    uri = "ws://localhost:8014/optimized-whisper"
    
    print("🚀 Testing Optimized Original Whisper")
    print("🎯 TARGET: Cut your 76s down to 25-35s")
    print("⚡ Method: Your working code + smart optimizations")
    print("✅ Proven approach, just faster")
    print("💰 Cost: FREE - No API costs!")
    
    try:
        async with websockets.connect(uri) as websocket:
            request = {
                "video_url": "https://www.youtube.com/watch?v=D1R-jKKp3NA",
                "chunk_duration_seconds": 10.0,  # Reasonable chunk size
                "max_concurrent_chunks": 16,     # Conservative concurrency
                "model_size": "tiny"             # Fastest model
            }
            
            start_time = time.time()
            await websocket.send(json.dumps(request))
            
            chunk_times = []
            download_time = None
            chunk_creation_time = None
            first_result_time = None
            
            async for message in websocket:
                data = json.loads(message)
                message_type = data.get("type")
                
                if message_type == "video_info":
                    title = data.get("title")
                    duration = data.get("duration")
                    optimization = data.get("optimization_level")
                    estimated = data.get("estimated_time")
                    max_concurrent = data.get("max_concurrent")
                    print(f"\n📹 Video: {title}")
                    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
                    print(f"🔧 Optimization: {optimization}")
                    print(f"👥 Workers: {max_concurrent}")
                    print(f"🎯 Estimated: {estimated}")
                    
                elif message_type == "download_complete":
                    download_time = data.get("download_time")
                    message = data.get("message")
                    print(f"\n📦 {message}")
                    
                elif message_type == "chunks_created":
                    total_chunks = data.get("total_chunks")
                    chunk_creation_time = data.get("chunk_creation_time")
                    estimated_processing = data.get("estimated_processing")
                    print(f"🔪 Created {total_chunks} chunks in {chunk_creation_time:.1f}s")
                    print(f"⏱️  Estimated processing: {estimated_processing}")
                    
                elif message_type == "first_result":
                    first_result_time = data.get("time_to_first_result")
                    message = data.get("message")
                    print(f"\n🎉 {message}")
                    
                elif message_type == "chunk_complete":
                    chunk_id = data.get("chunk_id")
                    text = data.get("text", "")
                    processing_time = data.get("processing_time")
                    start_time_chunk = data.get("start_time")
                    
                    chunk_times.append(processing_time)
                    
                    # Show first few and every 10th result
                    if chunk_id < 3 or chunk_id % 10 == 0:
                        print(f"✅ Chunk {chunk_id} ({start_time_chunk:.0f}s): '{text[:40]}...' ({processing_time:.1f}s)")
                    
                elif message_type == "progress":
                    completed = data.get("completed")
                    failed = data.get("failed")
                    total = data.get("total")
                    percent = data.get("progress_percent")
                    
                    # Show progress every 20% or near end
                    if completed % 15 == 0 or percent > 85:
                        print(f"📊 Progress: {completed}/{total} ({percent:.0f}%) - {failed} failed")
                    
                elif message_type == "transcription_complete":
                    successful = data.get("successful_chunks")
                    failed_chunks = data.get("failed_chunks")
                    avg_chunk_time = data.get("average_chunk_time")
                    full_transcript = data.get("full_transcript", "")
                    
                    print(f"\n🎉 OPTIMIZED TRANSCRIPTION COMPLETE!")
                    print(f"   Successful: {successful}, Failed: {failed_chunks}")
                    print(f"   Average chunk time: {avg_chunk_time:.2f}s")
                    print(f"   Transcript length: {len(full_transcript)} characters")
                    
                elif message_type == "performance":
                    total_time = data.get("total_time")
                    download_time = data.get("download_time")
                    chunk_creation_time = data.get("chunk_creation_time")
                    processing_time = data.get("processing_time")
                    speedup = data.get("speedup_factor")
                    target_achieved = data.get("target_achieved")
                    
                    print(f"\n🏆 OPTIMIZED RESULTS:")
                    print(f"   Total time: {total_time:.1f} seconds")
                    print(f"   Download: {download_time:.1f}s")
                    print(f"   Chunk creation: {chunk_creation_time:.1f}s") 
                    print(f"   Processing: {processing_time:.1f}s")
                    print(f"   Speedup: {speedup:.1f}x faster than real-time")
                    print(f"   35-second target: {'🎯 ACHIEVED!' if target_achieved else '⏳ CLOSE'}")
                    
                    # The critical comparison
                    baseline = 76.0
                    improvement = baseline / total_time
                    savings = baseline - total_time
                    
                    print(f"\n📈 IMPROVEMENT vs YOUR BASELINE:")
                    print(f"   Your original system: {baseline:.1f}s")
                    print(f"   Optimized system: {total_time:.1f}s")
                    print(f"   Speed improvement: {improvement:.1f}x FASTER! 🚀")
                    print(f"   Time saved per video: {savings:.1f} seconds")
                    
                    if total_time <= 25.0:
                        print(f"\n🏅 EXCELLENT! 3x speed improvement!")
                        print(f"💡 You just cut your time by over 50 seconds!")
                        print(f"🎯 This is a massive productivity boost!")
                    elif total_time <= 35.0:
                        print(f"\n⭐ GREAT! Major improvement achieved!")
                        print(f"💡 You more than doubled your speed!")
                    elif total_time <= 50.0:
                        print(f"\n✅ GOOD! Solid improvement!")
                        print(f"💡 Still much faster than before")
                    else:
                        print(f"\n🔧 Some improvement, but room for more")
                    
                    if chunk_times:
                        fastest = min(chunk_times)
                        avg = sum(chunk_times) / len(chunk_times)
                        print(f"\n📊 Processing Details:")
                        print(f"   Fastest chunk: {fastest:.1f}s")
                        print(f"   Average chunk: {avg:.1f}s")
                        print(f"   Total chunks: {len(chunk_times)}")
                        print(f"   Efficiency: {avg:.1f}s avg vs {avg_chunk_time:.1f}s target")
                    
                    print(f"   💰 Cost: $0.00 (FREE!)")
                    
                    break
                    
                elif message_type == "status":
                    message = data.get("message")
                    print(f"📝 {message}")
                    
                elif message_type == "error":
                    error_msg = data.get("message")
                    print(f"❌ Error: {error_msg}")
                    break
                    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        if "Connection refused" in str(e):
            print("💡 Make sure optimized_original_whisper.py is running on port 8014")
            print("💡 Run: python optimized_original_whisper.py")

if __name__ == "__main__":
    print("🚀 Optimized Original Whisper Test")
    print("✅ Based on your proven working approach")
    print("🎯 Goal: Cut 76 seconds down to 25-35 seconds")
    print("📋 Requirements: Just openai-whisper")
    print()
    
    asyncio.run(test_optimized_original())