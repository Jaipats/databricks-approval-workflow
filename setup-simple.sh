#!/bin/bash
# Simple setup script for Databricks Approval Workflow App

echo "🚀 Setting up Simplified Databricks Approval App..."

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv-simple

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv-simple/bin/activate

# Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# Install minimal dependencies
echo "📚 Installing minimal dependencies..."
pip install -r requirements-simple.txt

# Create local data directory
mkdir -p local_data

# Make run script executable
chmod +x run-simple.sh

echo ""
echo "✅ Simplified setup complete!"
echo ""
echo "📦 Installed packages:"
pip list | grep -E "(streamlit|pandas|databricks|requests)"
echo ""
echo "🎯 Next steps:"
echo "1. Run: ./run-simple.sh"
echo "2. Open browser to: http://localhost:8501"
echo ""
echo "✨ Benefits of simplified version:"
echo "   - No Java runtime required"
echo "   - 6 dependencies instead of 20+"
echo "   - Faster startup and operation"
echo "   - Same core functionality"
