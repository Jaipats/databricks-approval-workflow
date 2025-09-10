#!/bin/bash
# Run script for Simplified Databricks Approval Workflow App

echo "🚀 Starting Simplified Databricks Approval App..."

# Activate virtual environment
if [ -d "venv-simple" ]; then
    source venv-simple/bin/activate
    echo "✅ Virtual environment activated"
fi

# Load environment variables
if [ -f .env ]; then
    echo "📝 Loading environment variables"
    export $(grep -v '^#' .env | xargs)
fi

echo "🌐 Starting Streamlit application..."
echo "📱 App will be available at: http://localhost:8501"
echo ""
echo "Press Ctrl+C to stop the application"
echo ""

streamlit run app_simple.py \
    --server.port 8501 \
    --server.headless true \
    --browser.gatherUsageStats false
