#!/bin/bash
# Setup script for Databricks Approval Workflow App

echo "🚀 Setting up Databricks Approval Workflow App..."

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt

# Copy environment configuration
if [ ! -f .env ]; then
    cp .env.example .env
    echo "📝 Created .env file. Please update with your configuration."
else
    echo "✅ .env file already exists"
fi

# Create local data directory for testing
mkdir -p local_data

# Make scripts executable
chmod +x run.sh

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your Databricks workspace URL and token:"
echo "   nano .env"
echo ""
echo "2. Start the application:"
echo "   ./run.sh"
echo ""
echo "3. Open browser to:"
echo "   http://localhost:8501"
echo ""
echo "For help, see README.md"
