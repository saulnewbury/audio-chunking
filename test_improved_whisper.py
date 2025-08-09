import asyncio
import websockets
import json
import time

async def test_improved_whisper():
    """Test improved Whisper for maximum speed"""
    
    uri = "ws://localhost:8007/improved-whisper"
    
    print("🚀 Testing Improved Local Whisper Ultra-Fast Transcription")
    print("🎯 TARGET: Under 10 seconds for 15-minute video")
    print("⚡ Method: Optimized concurrent processing with 16+ workers")
    print("💰 Cost: FREE - No API costs!")
    
    try:
        async with websockets.connect(uri) as websocket:
            request = {
                "video_url": "https://www.youtube.com/watch?v=D1R-jKKp3NA",
                "chunk_duration_seconds": 12.0,  # Smaller chunks
                "max_concurrent_chunks": 16,     # More workers
                "model_size": "tiny",            # Fastest model
                "optimization_level": "max"     # Maximum speed optimizations
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
                    optimization = data.get("optimization_level")
                    max_concurrent = data.get("max_concurrent")
                    estimated = data.get("estimated_time")
                    print(f"\n📹 Video: {title}")
                    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
                    print(f"🧠 Model: {model_size}")
                    print(f"⚡ Optimization: {optimization}")
                    print(f"👥 Workers: {max_concurrent}")
                    print(f"🎯 Estimated: {estimated}")
                    
                elif message_type == "chunks_created":
                    total_chunks = data.get("total_chunks")
                    chunk_duration = data.get("chunk_duration")
                    estimated_processing = data.get("estimated_processing")
                    print(f"📦 Created {total_chunks} chunks ({chunk_duration}s each)")
                    print(f"⏱️  Estimated processing: {estimated_processing}")
                    print(f"🚀 Starting improved concurrent processing...")
                    
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
                    
                    if completed % 5 == 0 or percent > 90:  # Show every 5th completion or near end
                        print(f"📊 Progress: {completed}/{total} chunks ({percent:.1f}%) - {failed} failed")
                    
                elif message_type == "transcription_complete":
                    successful = data.get("successful_chunks")
                    failed_chunks = data.get("failed_chunks")
                    avg_chunk_time = data.get("average_chunk_time")
                    full_transcript = data.get("full_transcript", "")
                    
                    print(f"\n🎉 IMPROVED WHISPER COMPLETE!")
                    print(f"   Successful: {successful}")
                    print(f"   Failed: {failed_chunks}")
                    print(f"   Average chunk time: {avg_chunk_time:.2f}s")
                    print(f"   Transcript length: {len(full_transcript)} characters")
                    print(f"   First 200 chars: {full_transcript[:200]}...")
                    
                elif message_type == "performance":
                    total_time = data.get("total_time")
                    speedup = data.get("speedup_factor")
                    target_achieved = data.get("target_achieved")
                    optimization = data.get("optimization_level")
                    
                    print(f"\n🏆 IMPROVED WHISPER RESULTS:")
                    print(f"   Total time: {total_time:.2f} seconds")
                    print(f"   Speedup: {speedup:.1f}x faster than real-time")
                    print(f"   10-second target: {'🎯 ACHIEVED!' if target_achieved else '⏳ Close!'}")
                    print(f"   Optimization: {optimization}")
                    print(f"   Cost: $0.00 (FREE!)")
                    
                    if target_achieved:
                        print(f"\n🚀 SUCCESS! Improved Whisper hit the target!")
                        print(f"💡 {speedup:.0f}x faster than real-time processing")
                        print(f"🆓 Zero API costs")
                    else:
                        if chunk_times:
                            fastest = min(chunk_times)
                            avg = sum(chunk_times) / len(chunk_times)
                            print(f"\n🔧 Performance Analysis:")
                            print(f"   Fastest chunk: {fastest:.2f}s")
                            print(f"   Average chunk: {avg:.2f}s")
                            if total_time <= 15:
                                print(f"💡 Very close! Try even smaller chunks or more workers")
                            else:
                                print(f"💡 Try optimization_level='max' or check system resources")
                    
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

async def test_different_optimizations():
    """Test different optimization levels"""
    
    optimizations = [
        {"level": "max", "chunks": 10.0, "workers": 20},
        {"level": "balanced", "chunks": 15.0, "workers": 16},
        {"level": "quality", "chunks": 20.0, "workers": 12}
    ]
    
    print("🧪 Testing Different Optimization Levels")
    print("=" * 50)
    
    for i, opt in enumerate(optimizations, 1):
        print(f"\n🔄 Test {i}/3: {opt['level'].upper()} optimization")
        print(f"   Chunks: {opt['chunks']}s, Workers: {opt['workers']}")
        
        try:
            uri = "ws://localhost:8007/improved-whisper"
            async with websockets.connect(uri) as websocket:
                request = {
                    "video_url": "https://www.youtube.com/watch?v=D1R-jKKp3NA",
                    "chunk_duration_seconds": opt["chunks"],
                    "max_concurrent_chunks": opt["workers"],
                    "model_size": "tiny",
                    "optimization_level": opt["level"]
                }
                
                start_time = time.time()
                await websocket.send(json.dumps(request))
                
                async for message in websocket:
                    data = json.loads(message)
                    if data.get("type") == "performance":
                        total_time = data.get("total_time")
                        speedup = data.get("speedup_factor")
                        print(f"   ⏱️  Time: {total_time:.2f}s")
                        print(f"   ⚡ Speedup: {speedup:.1f}x")
                        break
                        
        except Exception as e:
            print(f"   ❌ Test failed: {e}")

if __name__ == "__main__":
    import sys
    
    print("🚀 Improved Local Whisper Test Suite")
    print("🎯 Make sure local_whisper_service.py is running on port 8007")
    print("📋 Requires: pip install openai-whisper")
    print()
    
    if len(sys.argv) > 1 and sys.argv[1] == "compare":
        asyncio.run(test_different_optimizations())
    else:
        asyncio.run(test_improved_whisper())