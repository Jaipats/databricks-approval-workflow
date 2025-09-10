#!/bin/bash
# Run script for Databricks Job Approval Workflow

set -e

echo "🚀 Starting Databricks Job Approval Workflow"
echo "=============================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Run ./setup.sh first"
    exit 1
fi

# Activate virtual environment
echo "🔗 Activating virtual environment..."
source venv/bin/activate

# Check if app.py exists
if [ ! -f "app.py" ]; then
    echo "❌ app.py not found"
    exit 1
fi

# Set environment variables for local development
export DATABRICKS_CATALOG=${DATABRICKS_CATALOG:-"main"}
export DATABRICKS_SCHEMA=${DATABRICKS_SCHEMA:-"approval_workflow"}
export USE_MOCK_JOBS=${USE_MOCK_JOBS:-"false"}

echo "⚙️ Environment Configuration:"
echo "   DATABRICKS_CATALOG: $DATABRICKS_CATALOG"
echo "   DATABRICKS_SCHEMA: $DATABRICKS_SCHEMA"
echo "   USE_MOCK_JOBS: $USE_MOCK_JOBS"
echo ""

# Start Streamlit app
echo "🌟 Starting Streamlit application..."
echo "📍 App will be available at: http://localhost:8501"
echo ""

streamlit run app.py \
  --server.port=8501 \
  --server.headless=false \
  --browser.gatherUsageStats=false \
  --server.enableCORS=false \
  --server.enableXsrfProtection=false