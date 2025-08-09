import asyncio
import websockets
import json
import time

async def test_whisper_fast():
    """Test ultra-fast Whisper transcription"""
    
    uri = "ws://localhost:8005/whisper-fast"
    
    print("🚀 Testing Whisper Ultra-Fast Transcription")
    print("🎯 Target: 5 seconds for any video length")
    print("⚡ Method: OpenAI Whisper API with concurrency")
    
    try:
        async with websockets.connect(uri) as websocket:
            request = {
                "video_url": "https://www.youtube.com/watch?v=D1R-jKKp3NA",  # Steve Jobs speech
                "chunk_duration_seconds": 60.0,  # 60-second chunks
                "max_concurrent_chunks": 20      # Process 20 chunks simultaneously
            }
            
            start_time = time.time()
            await websocket.send(json.dumps(request))
            
            completed_chunks = 0
            total_chunks = 0
            chunk_times = []
            
            async for message in websocket:
                data = json.loads(message)
                message_type = data.get("type")
                
                if message_type == "video_info":
                    title = data.get("title")
                    duration = data.get("duration")
                    estimated_time = data.get("estimated_time")
                    print(f"\n📹 Video: {title}")
                    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
                    print(f"🎯 Estimated processing time: {estimated_time}")
                    
                elif message_type == "chunks_created":
                    total_chunks = data.get("total_chunks")
                    estimated_processing = data.get("estimated_processing")
                    print(f"📦 Created {total_chunks} chunks")
                    print(f"⏱️  Estimated processing: {estimated_processing}")
                    print(f"🚀 Starting concurrent Whisper processing...")
                    
                elif message_type == "chunk_complete":
                    completed_chunks += 1
                    chunk_id = data.get("chunk_id")
                    text = data.get("text", "")
                    processing_time = data.get("processing_time")
                    start_time_chunk = data.get("start_time")
                    
                    chunk_times.append(processing_time)
                    
                    # Show first few results
                    if completed_chunks <= 3:
                        print(f"✅ Chunk {chunk_id} ({start_time_chunk:.0f}s): '{text[:60]}...' ({processing_time:.2f}s)")
                    
                elif message_type == "progress":
                    completed = data.get("completed")
                    failed = data.get("failed")
                    total = data.get("total")
                    percent = data.get("progress_percent")
                    
                    print(f"📊 Progress: {completed}/{total} chunks ({percent:.1f}%) - {failed} failed")
                    
                elif message_type == "transcription_complete":
                    full_transcript = data.get("full_transcript", "")
                    successful = data.get("successful_chunks")
                    failed_chunks = data.get("failed_chunks")
                    avg_chunk_time = data.get("average_chunk_time")
                    
                    print(f"\n🎉 TRANSCRIPTION COMPLETE!")
                    print(f"   Successful chunks: {successful}")
                    print(f"   Failed chunks: {failed_chunks}")
                    print(f"   Average chunk processing time: {avg_chunk_time:.2f}s")
                    print(f"   Transcript length: {len(full_transcript)} characters")
                    print(f"   First 200 chars: {full_transcript[:200]}...")
                    
                elif message_type == "performance":
                    total_time = data.get("total_time")
                    speedup = data.get("speedup_factor")
                    target_achieved = data.get("target_achieved")
                    
                    print(f"\n⚡ WHISPER PERFORMANCE RESULTS:")
                    print(f"   Total processing time: {total_time:.2f} seconds")
                    print(f"   Speedup factor: {speedup:.1f}x")
                    print(f"   Target achieved (≤5s): {'🏆 YES!' if target_achieved else '❌ NO'}")
                    
                    if target_achieved:
                        print(f"🚀 EXCELLENT! Whisper achieved ultra-fast transcription!")
                        print(f"💡 Processed {speedup:.0f}x faster than real-time")
                    else:
                        print(f"🔧 Close! Try increasing max_concurrent_chunks")
                        if chunk_times:
                            fastest_chunk = min(chunk_times)
                            print(f"💡 Fastest chunk: {fastest_chunk:.2f}s - system is capable of faster processing")
                    
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

if __name__ == "__main__":
    print("🚀 Whisper Ultra-Fast Transcription Test")
    print("🎯 Make sure whisper_fast_service.py is running on port 8005")
    print("🔑 Make sure OPENAI_API_KEY is set in .env file")
    print()
    asyncio.run(test_whisper_fast())