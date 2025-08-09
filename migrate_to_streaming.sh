#!/bin/bash
echo "🔄 Migrating Your Existing Service to Real-Time Streaming"
echo "========================================================="

# Check if we're in the right directory
if [[ ! -f "main.py" ]] || [[ ! -f "requirements.txt" ]]; then
    echo "❌ Please run this from your service directory with main.py and requirements.txt"
    exit 1
fi

echo "✅ Found existing service files"

# Create backup
mkdir -p backup_$(date +%Y%m%d_%H%M%S)
cp main.py backup_*/
cp requirements.txt backup_*/
cp Dockerfile backup_*/ 2>/dev/null || true

# Check Python
python3 --version || { echo "❌ Python 3 required"; exit 1; }

# Create/activate venv
if [[ ! -d "venv" ]]; then
    python3 -m venv venv
fi
source venv/bin/activate

# Install ffmpeg if needed
if ! command -v ffmpeg &> /dev/null; then
    echo "Installing ffmpeg..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install ffmpeg
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo apt-get update && sudo apt-get install -y ffmpeg
    fi
fi

# Update requirements
cat > requirements.txt << 'REQS'
fastapi>=0.100.0
uvicorn[standard]>=0.23.0
websockets>=11.0.0
aiohttp>=3.8.0
aiofiles>=23.0.0
yt-dlp>=2023.7.6
pydub>=0.25.1
pydantic>=2.8.0
python-multipart>=0.0.6
python-dotenv>=1.0.0
numpy>=1.21.0
soundfile>=0.12.0
REQS

pip install -r requirements.txt

# Create .env if not exists
if [[ ! -f .env ]]; then
    cat > .env << 'ENV'
ASSEMBLYAI_API_KEY=your_assemblyai_api_key_here
PORT=8002
HOST=0.0.0.0
LOG_LEVEL=info
ALLOWED_ORIGINS=http://localhost:3000,https://your-domain.com
ENV
fi

# Create helper scripts
cat > start.sh << 'START'
#!/bin/bash
source venv/bin/activate
python main.py
START

cat > stop.sh << 'STOP'
#!/bin/bash
kill -9 $(lsof -ti:8002) 2>/dev/null || true
STOP

chmod +x *.sh

echo "✅ Migration setup complete!"
echo "Next: Replace main.py content and set your API key in .env"
