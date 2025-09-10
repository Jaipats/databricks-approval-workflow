#!/bin/bash  
# Run script for Databricks Approval Workflow App

echo "🚀 Starting Databricks Approval Workflow App..."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "✅ Virtual environment activated"
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  Warning: .env file not found. Using default configuration."
    echo "   Copy .env.example to .env and update with your settings."
    echo ""
fi

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "📝 Loading environment variables from .env"
    export $(grep -v '^#' .env | xargs)
fi

# Check for required environment variables
if [ -z "$DATABRICKS_HOST" ] || [ "$DATABRICKS_HOST" = "https://your-workspace.cloud.databricks.com" ]; then
    echo "⚠️  Warning: DATABRICKS_HOST not configured properly"
    echo "   Please update .env file with your Databricks workspace URL"
fi

if [ -z "$DATABRICKS_TOKEN" ] || [ "$DATABRICKS_TOKEN" = "your-databricks-access-token" ]; then
    echo "⚠️  Warning: DATABRICKS_TOKEN not configured"
    echo "   Please update .env file with your Databricks access token"
    echo ""
    echo "To generate a token:"
    echo "1. Go to your Databricks workspace"
    echo "2. Click your profile > User Settings > Access tokens"
    echo "3. Generate new token and copy to .env file"
    echo ""
fi

# Start Streamlit app
echo "🌐 Starting Streamlit application..."
echo "📱 App will be available at: http://localhost:8501"
echo ""
echo "Press Ctrl+C to stop the application"
echo ""

streamlit run app.py \
    --server.port 8501 \
    --server.headless true \
    --server.enableCORS false \
    --browser.gatherUsageStats false
