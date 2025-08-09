"""
Updated Test client for Universal-Streaming (v3) API
Compatible with the new AssemblyAI Universal-Streaming service
"""

import asyncio
import websockets
import json
import requests
import time
from typing import AsyncGenerator

class UniversalStreamingClient:
    def __init__(self, base_url: str = "ws://localhost:8002"):
        self.base_url = base_url
        self.http_base = base_url.replace("ws://", "http://").replace("wss://", "https://")
    
    def test_health_check(self):
        """Test health endpoint"""
        try:
            response = requests.get(f"{self.http_base}/health")
            print(f"🏥 Health check: {response.json()}")
            return response.status_code == 200
        except Exception as e:
            print(f"❌ Health check failed: {e}")
            return False

    async def test_websocket_streaming(self, video_url: str):
        """Test WebSocket streaming endpoint with Universal-Streaming"""
        print(f"🚀 Testing WebSocket streaming with: {video_url}")
        print("-" * 60)
        
        uri = f"{self.base_url}/stream-transcribe"
        
        try:
            async with websockets.connect(uri) as websocket:
                # Send initial request
                request = {
                    "video_url": video_url,
                    "chunk_duration_ms": 200,
                    "enable_formatting": True,
                    "sample_rate": 16000
                }
                
                await websocket.send(json.dumps(request))
                print("✅ Request sent, waiting for responses...")
                
                transcript_parts = []
                start_time = time.time()
                
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        message_type = data.get("type")
                        
                        if message_type == "connection_established":
                            print("🔗 Connected to Universal-Streaming")
                            
                        elif message_type == "session_started":
                            print("🎯 Universal-Streaming session started")
                            
                        elif message_type == "video_info":
                            print(f"📹 Video: {data['title']} ({data['duration']}s)")
                            
                        elif message_type == "download_complete":
                            print(f"⬇️ Audio downloaded: {data['audio_file']}")
                            
                        elif message_type == "progress":
                            progress = data.get("progress_percent", 0)
                            duration = data.get("duration_processed", 0)
                            print(f"⏳ Progress: {progress:.1f}% ({duration:.1f}s processed)")
                            
                        elif message_type == "transcript":
                            # Handle Universal-Streaming transcript format
                            text = data.get("text", "")
                            end_of_turn = data.get("end_of_turn", False)
                            turn_order = data.get("turn_order", 0)
                            confidence = data.get("confidence", 0)
                            is_formatted = data.get("is_formatted", False)
                            
                            if end_of_turn:
                                transcript_parts.append(text)
                                format_indicator = " [FORMATTED]" if is_formatted else ""
                                print(f"📝 TURN {turn_order}: {text} (confidence: {confidence:.2f}){format_indicator}")
                            else:
                                print(f"📝 partial turn {turn_order}: {text}")
                                
                        elif message_type == "audio_complete":
                            print("🎵 Audio processing complete, waiting for final transcripts...")
                            
                        elif message_type == "session_ended":
                            print("🎯 Universal-Streaming session ended")
                            
                        elif message_type == "complete":
                            total_time = time.time() - start_time
                            final_text = data.get("final_text", "")
                            print(f"\n🎉 TRANSCRIPTION COMPLETE in {total_time:.2f}s")
                            print(f"📄 Total transcripts: {data.get('total_transcripts', 0)}")
                            print(f"📋 Final text: {final_text}")
                            break
                            
                        elif message_type == "error":
                            print(f"❌ Error: {data.get('message', 'Unknown error')}")
                            break
                            
                    except json.JSONDecodeError:
                        print(f"⚠️ Invalid JSON: {message}")
                    except Exception as e:
                        print(f"⚠️ Error processing message: {e}")
                
                print("\n" + "="*60)
                print("FINAL TRANSCRIPT:")
                print(" ".join(transcript_parts))
                print("="*60)
                
        except Exception as e:
            print(f"❌ WebSocket connection failed: {e}")

    def test_http_streaming(self, video_url: str):
        """Test HTTP streaming endpoint"""
        print(f"🌐 Testing HTTP streaming with: {video_url}")
        print("-" * 60)
        
        url = f"{self.http_base}/stream-transcribe"
        data = {
            "url": video_url,
            "chunk_duration_ms": 200,
            "enable_formatting": True,
            "sample_rate": 16000
        }
        
        try:
            response = requests.post(url, json=data, stream=True)
            
            if response.status_code != 200:
                print(f"❌ HTTP error: {response.status_code} - {response.text}")
                return
            
            print("✅ HTTP stream started...")
            transcript_parts = []
            start_time = time.time()
            
            for line in response.iter_lines():
                if line:
                    line = line.decode('utf-8')
                    if line.startswith('data: '):
                        try:
                            data = json.loads(line[6:])  # Remove 'data: ' prefix
                            message_type = data.get("type")
                            
                            if message_type == "connection_established":
                                print("🔗 Connected to Universal-Streaming")
                                
                            elif message_type == "session_started":
                                print("🎯 Session started")
                                
                            elif message_type == "video_info":
                                print(f"📹 Video: {data['title']} ({data['duration']}s)")
                                
                            elif message_type == "progress":
                                progress = data.get("progress_percent", 0)
                                print(f"⏳ Progress: {progress:.1f}%")
                                
                            elif message_type == "transcript":
                                text = data.get("text", "")
                                end_of_turn = data.get("end_of_turn", False)
                                turn_order = data.get("turn_order", 0)
                                
                                if end_of_turn:
                                    transcript_parts.append(text)
                                    print(f"📝 TURN {turn_order}: {text}")
                                else:
                                    print(f"📝 partial: {text}")
                                    
                            elif message_type == "complete":
                                total_time = time.time() - start_time
                                print(f"\n🎉 COMPLETE in {total_time:.2f}s")
                                break
                                
                            elif message_type == "error":
                                print(f"❌ Error: {data.get('message', 'Unknown error')}")
                                break
                                
                        except json.JSONDecodeError:
                            print(f"⚠️ Invalid JSON in line: {line}")
                        except Exception as e:
                            print(f"⚠️ Error processing line: {e}")
            
            print("\n" + "="*60)
            print("FINAL TRANSCRIPT:")
            print(" ".join(transcript_parts))
            print("="*60)
            
        except Exception as e:
            print(f"❌ HTTP streaming failed: {e}")


def run_quick_test():
    """Run a quick synchronous test"""
    client = UniversalStreamingClient()
    
    print("🚀 Quick Health Check")
    if client.test_health_check():
        print("✅ Service is running and healthy!")
        return True
    else:
        print("❌ Service is not responding. Check if it's running on port 8002")
        return False


async def run_full_test():
    """Run full streaming test with Universal-Streaming"""
    client = UniversalStreamingClient()
    
    # Test with a short video (Rick Roll is long, let's use a shorter one)
    short_video = "https://www.youtube.com/watch?v=jNQXAC9IVRw"  # "Me at the zoo" - 19 seconds
    # Alternative: Rick Roll (but it's 3+ minutes)
    # short_video = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    
    print(f"🎬 Testing Universal-Streaming with: {short_video}")
    print("This is the first YouTube video ever uploaded (19 seconds)")
    await client.test_websocket_streaming(short_video)


async def run_quick_websocket_test():
    """Quick WebSocket connection test"""
    client = UniversalStreamingClient()
    
    # Very short test video
    test_video = "https://www.youtube.com/watch?v=jNQXAC9IVRw"  # 19 seconds
    
    print("🚀 Quick WebSocket Test (19 second video)")
    await client.test_websocket_streaming(test_video)


def test_direct_service():
    """Test service endpoints directly"""
    print("🔍 Testing service endpoints...")
    
    # Test root endpoint
    try:
        response = requests.get("http://localhost:8002/")
        print(f"📡 Root endpoint: {response.json()}")
    except Exception as e:
        print(f"❌ Root endpoint failed: {e}")
    
    # Test health endpoint
    try:
        response = requests.get("http://localhost:8002/health")
        health_data = response.json()
        print(f"🏥 Health endpoint: {health_data}")
        
        # Check API key status
        if health_data.get('assemblyai_configured'):
            print("✅ AssemblyAI API key is configured")
        else:
            print("❌ AssemblyAI API key not configured properly")
            
        # Check API version
        api_version = health_data.get('api_version', 'unknown')
        print(f"📡 API Version: {api_version}")
        
    except Exception as e:
        print(f"❌ Health endpoint failed: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "quick":
            if run_quick_test():
                print("\n🎯 Service is ready for streaming tests!")
                print("Next steps:")
                print("• python test_client.py websocket  # Quick WebSocket test")
                print("• python test_client.py full       # Full streaming test")
                
        elif command == "websocket":
            print("🚀 Quick WebSocket Test with Universal-Streaming")
            try:
                asyncio.run(run_quick_websocket_test())
            except KeyboardInterrupt:
                print("\n❌ Test cancelled by user")
            except Exception as e:
                print(f"\n❌ Test failed: {e}")
                
        elif command == "full":
            print("🚀 Full Universal-Streaming Test")
            print("Make sure you have:")
            print("1. Set ASSEMBLYAI_API_KEY in your .env file")
            print("2. Started the Universal-Streaming service on port 8002")
            print("3. Have a stable internet connection")
            print("\nPress Ctrl+C to cancel, or wait 3 seconds to continue...")
            
            try:
                time.sleep(3)
                asyncio.run(run_full_test())
            except KeyboardInterrupt:
                print("\n❌ Test cancelled by user")
            except Exception as e:
                print(f"\n❌ Test failed: {e}")
                
        elif command == "endpoints":
            test_direct_service()
            
        else:
            print("❌ Unknown command")
            print("Usage:")
            print("  python test_client.py quick      # Health check")
            print("  python test_client.py websocket  # Quick WebSocket test")
            print("  python test_client.py full       # Full streaming test")
            print("  python test_client.py endpoints  # Test service endpoints")
    
    else:
        print("🧪 Universal-Streaming Test Client")
        print("Usage:")
        print("  python test_client.py quick      # Health check")
        print("  python test_client.py websocket  # Quick WebSocket test (19s video)")
        print("  python test_client.py full       # Full streaming test")
        print("  python test_client.py endpoints  # Test service endpoints")
        print("")
        print("🎯 Start with 'quick' to check if service is running")