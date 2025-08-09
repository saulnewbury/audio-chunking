# Dockerfile for Real-Time Streaming Transcription Service
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY .env* ./

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8002

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8002/health || exit 1

# Start command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]

---

# docker-compose.yml
version: '3.8'

services:
  streaming-transcription:
    build: .
    ports:
      - "8002:8002"
    environment:
      - ASSEMBLYAI_API_KEY=${ASSEMBLYAI_API_KEY}
      - PORT=8002
      - HOST=0.0.0.0
      - LOG_LEVEL=info
    volumes:
      - /tmp:/tmp  # For temporary file processing
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8002/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s

  # Optional: Add nginx for production
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - streaming-transcription
    restart: unless-stopped

---

# start.sh - Development start script
#!/bin/bash

echo "🚀 Starting Real-Time Streaming Transcription Service"

# Check if virtual environment exists
if [[ ! -d "venv" ]]; then
    echo "❌ Virtual environment not found. Run ./setup.sh first"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Check for .env file
if [[ ! -f .env ]]; then
    echo "❌ .env file not found. Run ./setup.sh first"
    exit 1
fi

# Check for API key
if ! grep -q "ASSEMBLYAI_API_KEY=.*[^_here]" .env; then
    echo "⚠️  WARNING: AssemblyAI API key not set in .env file"
    echo "Please edit .env and add your API key from https://www.assemblyai.com/app/account"
    read -p "Continue anyway? (y/N): " confirm
    if [[ ! $confirm =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "🔧 Starting development server..."
echo "📡 Service will be available at http://localhost:8002"
echo "📚 API docs at http://localhost:8002/docs"
echo "🏥 Health check at http://localhost:8002/health"
echo ""
echo "Press Ctrl+C to stop"

# Start the service
python main.py

---

# start_docker.sh - Docker start script
#!/bin/bash

echo "🐳 Starting with Docker"

# Check for .env file
if [[ ! -f .env ]]; then
    echo "❌ .env file not found"
    echo "Please create .env file with your ASSEMBLYAI_API_KEY"
    exit 1
fi

# Load environment variables
source .env

if [[ -z "$ASSEMBLYAI_API_KEY" ]] || [[ "$ASSEMBLYAI_API_KEY" == "your_assemblyai_api_key_here" ]]; then
    echo "❌ Please set ASSEMBLYAI_API_KEY in .env file"
    exit 1
fi

echo "🔨 Building Docker image..."
docker build -t streaming-transcription .

echo "🚀 Starting container..."
docker run -d \
    --name streaming-transcription \
    -p 8002:8002 \
    -e ASSEMBLYAI_API_KEY="$ASSEMBLYAI_API_KEY" \
    --restart unless-stopped \
    streaming-transcription

echo "✅ Container started!"
echo "📡 Service available at http://localhost:8002"
echo "🔍 Check logs: docker logs -f streaming-transcription"
echo "🛑 Stop service: docker stop streaming-transcription"

---

# test.sh - Quick test script
#!/bin/bash

echo "🧪 Quick Service Test"

# Test health endpoint
echo "1. Testing health endpoint..."
response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8002/health)

if [[ $response == "200" ]]; then
    echo "✅ Service is healthy"
else
    echo "❌ Service not responding (HTTP $response)"
    echo "Make sure the service is running on port 8002"
    exit 1
fi

# Run Python test client
echo "2. Running full test client..."
if [[ -f "venv/bin/activate" ]]; then
    source venv/bin/activate
fi

python test_client.py quick

echo "3. To run full streaming test:"
echo "   python test_client.py"

---

# stop.sh - Stop services
#!/bin/bash

echo "🛑 Stopping services..."

# Stop Docker container if running
if docker ps | grep -q streaming-transcription; then
    echo "🐳 Stopping Docker container..."
    docker stop streaming-transcription
    docker rm streaming-transcription
fi

# Kill any Python processes on port 8002
if lsof -ti:8002 > /dev/null 2>&1; then
    echo "🐍 Stopping Python service on port 8002..."
    kill -9 $(lsof -ti:8002) 2>/dev/null || true
fi

echo "✅ All services stopped"

---

# nginx.conf - Production nginx configuration
events {
    worker_connections 1024;
}

http {
    upstream streaming_backend {
        server streaming-transcription:8002;
    }

    map $http_upgrade $connection_upgrade {
        default upgrade;
        '' close;
    }

    server {
        listen 80;
        server_name localhost;

        # WebSocket support
        location /stream-transcribe {
            proxy_pass http://streaming_backend;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            
            # Increase timeouts for long transcriptions
            proxy_read_timeout 3600s;
            proxy_send_timeout 3600s;
        }

        # HTTP API endpoints
        location / {
            proxy_pass http://streaming_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            
            # For streaming responses
            proxy_buffering off;
            proxy_cache off;
        }
    }
}