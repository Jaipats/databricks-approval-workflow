"""
Databricks deployment configuration and setup scripts
"""

import os
import json
from typing import Dict, List, Any

class DatabricksDeployment:
    """Handle Databricks deployment tasks"""
    
    def __init__(self, workspace_url: str, token: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    def create_app_deployment_config(self) -> Dict[str, Any]:
        """Create configuration for Databricks App deployment"""
        return {
            "name": "databricks-approval-workflow",
            "description": "Job approval workflow application for Databricks",
            "config": {
                "command": ["python", "-m", "streamlit", "run", "app.py", "--server.port=8501"],
                "env": [
                    {"name": "STREAMLIT_SERVER_HEADLESS", "value": "true"},
                    {"name": "STREAMLIT_SERVER_ENABLE_CORS", "value": "false"},
                    {"name": "STREAMLIT_BROWSER_GATHER_USAGE_STATS", "value": "false"}
                ],
                "resources": {
                    "requests": {
                        "memory": "2Gi",
                        "cpu": "1000m"
                    },
                    "limits": {
                        "memory": "4Gi", 
                        "cpu": "2000m"
                    }
                },
                "source_code_path": "/Workspace/Users/{user_email}/approval-workflow-app"
            }
        }
    
    def create_job_definitions(self) -> List[Dict[str, Any]]:
        """Create sample job definitions for testing"""
        
        # Job 1: Data Processing
        job1_config = {
            "name": "Data Processing Job - Approval Required",
            "timeout_seconds": 3600,
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "data_processing",
                    "description": "Process raw data for analysis",
                    "notebook_task": {
                        "notebook_path": "/Workspace/Shared/jobs/data_processing_notebook",
                        "base_parameters": {
                            "input_path": "/mnt/raw-data/",
                            "output_path": "/mnt/processed-data/",
                            "processing_date": "{{job.start_time}}"
                        }
                    },
                    "new_cluster": {
                        "spark_version": "13.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 2,
                        "spark_conf": {
                            "spark.databricks.delta.preview.enabled": "true"
                        }
                    }
                }
            ],
            "email_notifications": {
                "on_success": ["data-team@company.com"],
                "on_failure": ["data-team@company.com", "ops-team@company.com"]
            },
            "tags": {
                "environment": "production",
                "team": "data-engineering",
                "approval_required": "true"
            }
        }
        
        # Job 2: ML Model Training  
        job2_config = {
            "name": "ML Model Training - Auto Triggered",
            "timeout_seconds": 7200,
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "model_training",
                    "description": "Train ML model with processed data",
                    "notebook_task": {
                        "notebook_path": "/Workspace/Shared/jobs/ml_training_notebook",
                        "base_parameters": {
                            "training_data_path": "/mnt/processed-data/",
                            "model_output_path": "/mnt/models/",
                            "experiment_name": "/Shared/ml-experiments/approval-workflow"
                        }
                    },
                    "new_cluster": {
                        "spark_version": "13.3.x-cpu-ml-scala2.12",
                        "node_type_id": "i3.xlarge", 
                        "num_workers": 3,
                        "spark_conf": {
                            "spark.databricks.delta.preview.enabled": "true"
                        }
                    }
                }
            ],
            "email_notifications": {
                "on_success": ["ml-team@company.com"],
                "on_failure": ["ml-team@company.com", "ops-team@company.com"]
            },
            "tags": {
                "environment": "production",
                "team": "ml-engineering",
                "triggered_by_approval": "true"
            }
        }
        
        return [job1_config, job2_config]
    
    def create_notebook_templates(self) -> Dict[str, str]:
        """Create sample notebook templates"""
        
        data_processing_notebook = '''
# Databricks notebook source
# MAGIC %md
# MAGIC # Data Processing Job - Requires Approval
# MAGIC
# MAGIC This notebook processes raw data and requires approval before the ML training job can run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Parameters

# COMMAND ----------

dbutils.widgets.text("input_path", "/mnt/raw-data/", "Input Path")
dbutils.widgets.text("output_path", "/mnt/processed-data/", "Output Path")  
dbutils.widgets.text("processing_date", "", "Processing Date")

input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")
processing_date = dbutils.widgets.get("processing_date")

print(f"Processing data from: {input_path}")
print(f"Output will be saved to: {output_path}")
print(f"Processing date: {processing_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Processing Logic

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime

# Read raw data
try:
    df_raw = spark.read.format("delta").load(input_path)
    print(f"Successfully loaded {df_raw.count()} records")
except Exception as e:
    print(f"Error loading data: {str(e)}")
    dbutils.notebook.exit("FAILED - Could not load input data")

# Data processing steps
df_processed = df_raw \\
    .filter(F.col("status") == "active") \\
    .withColumn("processed_timestamp", F.current_timestamp()) \\
    .withColumn("processing_date", F.lit(processing_date))

# Data quality checks
record_count = df_processed.count()
if record_count == 0:
    print("WARNING: No records after processing")
    dbutils.notebook.exit("FAILED - No records after processing")

print(f"Processed {record_count} records")

# COMMAND ----------

# MAGIC %md  
# MAGIC ## Save Processed Data

# COMMAND ----------

# Write processed data
try:
    df_processed.write \\
        .format("delta") \\
        .mode("overwrite") \\
        .option("overwriteSchema", "true") \\
        .save(output_path)
    
    print(f"Data successfully saved to {output_path}")
    
    # Log processing completion
    spark.sql(f"""
        INSERT INTO main.approval_workflow.job_audit_log 
        VALUES (
            '{processing_date}_{record_count}',
            'data_processing_job',
            'data_processing', 
            NULL,
            'job1',
            'completed',
            'completed',
            'Processed {record_count} records',
            'system',
            current_timestamp()
        )
    """)
    
    dbutils.notebook.exit("SUCCESS")
    
except Exception as e:
    print(f"Error saving data: {str(e)}")
    dbutils.notebook.exit("FAILED - Could not save processed data")
'''

        ml_training_notebook = '''
# Databricks notebook source
# MAGIC %md
# MAGIC # ML Model Training Job - Auto Triggered After Approval
# MAGIC
# MAGIC This notebook trains an ML model using the processed data after approval is granted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Parameters

# COMMAND ----------

dbutils.widgets.text("training_data_path", "/mnt/processed-data/", "Training Data Path")
dbutils.widgets.text("model_output_path", "/mnt/models/", "Model Output Path")
dbutils.widgets.text("experiment_name", "/Shared/ml-experiments/approval-workflow", "MLflow Experiment")

training_data_path = dbutils.widgets.get("training_data_path")
model_output_path = dbutils.widgets.get("model_output_path")
experiment_name = dbutils.widgets.get("experiment_name")

print(f"Training data path: {training_data_path}")
print(f"Model output path: {model_output_path}")
print(f"MLflow experiment: {experiment_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Training Data

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import pandas as pd

# Set MLflow experiment
mlflow.set_experiment(experiment_name)

# Load processed data
try:
    df_training = spark.read.format("delta").load(training_data_path)
    print(f"Loaded {df_training.count()} records for training")
    
    # Convert to pandas for sklearn
    pdf_training = df_training.toPandas()
    
except Exception as e:
    print(f"Error loading training data: {str(e)}")
    dbutils.notebook.exit("FAILED - Could not load training data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training

# COMMAND ----------

with mlflow.start_run() as run:
    # Prepare features (simplified example)
    # In practice, you would have proper feature engineering
    feature_cols = [col for col in pdf_training.columns if col not in ['target', 'processed_timestamp', 'processing_date']]
    X = pdf_training[feature_cols]
    y = pdf_training['target'] if 'target' in pdf_training.columns else pdf_training.iloc[:, 0]  # Mock target
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Train model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Evaluate model
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"Model accuracy: {accuracy:.4f}")
    
    # Log parameters and metrics
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("training_records", len(X_train))
    mlflow.log_metric("accuracy", accuracy)
    
    # Log model
    mlflow.sklearn.log_model(model, "model")
    
    # Save model to specified path
    model_path = f"{model_output_path}/model_{run.info.run_id}"
    mlflow.sklearn.save_model(model, model_path)
    
    print(f"Model saved to: {model_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training Completion

# COMMAND ----------

# Log training completion
try:
    spark.sql(f"""
        INSERT INTO main.approval_workflow.job_audit_log 
        VALUES (
            '{run.info.run_id}',
            'ml_training_job',
            'ml_training',
            NULL,
            'job2', 
            'completed',
            'completed',
            'Model trained with accuracy: {accuracy:.4f}',
            'system',
            current_timestamp()
        )
    """)
    
    print("Training completed successfully")
    dbutils.notebook.exit("SUCCESS")
    
except Exception as e:
    print(f"Error logging completion: {str(e)}")
    dbutils.notebook.exit("SUCCESS")  # Still success even if logging fails
'''

        return {
            "data_processing_notebook.py": data_processing_notebook,
            "ml_training_notebook.py": ml_training_notebook
        }


def generate_deployment_scripts():
    """Generate deployment shell scripts"""
    
    setup_script = '''#!/bin/bash
# Setup script for Databricks Approval Workflow App

echo "🚀 Setting up Databricks Approval Workflow App..."

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
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

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Update .env file with your Databricks workspace URL and token"
echo "2. Run: streamlit run app.py"
echo "3. Open browser to http://localhost:8501"
'''

    run_script = '''#!/bin/bash  
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
fi

# Start Streamlit app
streamlit run app.py --server.port 8501 --server.headless true

echo "📱 App should be available at: http://localhost:8501"
'''

    return {
        "setup.sh": setup_script,
        "run.sh": run_script
    }
