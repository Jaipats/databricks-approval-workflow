#!/usr/bin/env python3
"""
Databricks Apps Deployment Script
Automates the deployment of the Job Approval Workflow to Databricks Apps
"""

import os
import json
import subprocess
import sys
from pathlib import Path

def run_command(command, check=True):
    """Run shell command with error handling"""
    print(f"🔄 Running: {command}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=check)
        if result.stdout:
            print(result.stdout)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        return False

def check_databricks_cli():
    """Check if Databricks CLI is installed and configured"""
    print("🔍 Checking Databricks CLI...")
    
    # Check if CLI is installed
    if not run_command("databricks --version", check=False):
        print("❌ Databricks CLI not found. Installing...")
        if not run_command("curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"):
            print("❌ Failed to install Databricks CLI")
            return False
    
    # Check if CLI is configured
    if not run_command("databricks auth describe", check=False):
        print("⚠️ Databricks CLI not configured. Please run:")
        print("   databricks auth login --host <your-databricks-host>")
        return False
    
    print("✅ Databricks CLI is ready")
    return True

def setup_unity_catalog(catalog="main", schema="approval_workflow"):
    """Set up Unity Catalog schema and permissions"""
    print(f"🗄️ Setting up Unity Catalog: {catalog}.{schema}")
    
    commands = [
        f"databricks sql-exec --sql 'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}'",
        f"databricks sql-exec --sql 'DESCRIBE SCHEMA {catalog}.{schema}'"
    ]
    
    for cmd in commands:
        if not run_command(cmd, check=False):
            print(f"⚠️ Warning: Command failed: {cmd}")
    
    print("✅ Unity Catalog setup completed")

def validate_app_files():
    """Validate that all required files exist"""
    print("📋 Validating app files...")
    
    required_files = [
        "app.py",
        "backend/storage.py", 
        "backend/job_manager.py",
        "requirements.txt",
        "app_config.yml"
    ]
    
    missing_files = []
    for file in required_files:
        if not Path(file).exists():
            missing_files.append(file)
    
    if missing_files:
        print("❌ Missing required files:")
        for file in missing_files:
            print(f"   - {file}")
        return False
    
    print("✅ All required files found")
    return True

def create_app_bundle():
    """Create deployment bundle for Databricks Apps"""
    print("📦 Creating app bundle...")
    
    bundle_dir = Path("databricks_app_bundle")
    if bundle_dir.exists():
        run_command(f"rm -rf {bundle_dir}")
    
    bundle_dir.mkdir(exist_ok=True)
    
    # Copy required files
    files_to_copy = [
        "databricks_app.py",
        "backend/",
        "databricks_requirements.txt",
        "databricks_app_config.yml",
        "README-DATABRICKS-APPS.md"
    ]
    
    for item in files_to_copy:
        if Path(item).is_file():
            run_command(f"cp {item} {bundle_dir}/")
        elif Path(item).is_dir():
            run_command(f"cp -r {item} {bundle_dir}/")
    
    print(f"✅ App bundle created in {bundle_dir}")
    return bundle_dir

def deploy_to_databricks_apps(bundle_dir, app_name="databricks-job-approval-workflow"):
    """Deploy to Databricks Apps"""
    print(f"🚀 Deploying {app_name} to Databricks Apps...")
    
    # Change to bundle directory
    original_cwd = os.getcwd()
    os.chdir(bundle_dir)
    
    try:
        # Check if app already exists
        app_exists = run_command(f"databricks apps get {app_name}", check=False)
        
        if app_exists:
            print(f"📝 Updating existing app: {app_name}")
            success = run_command(f"databricks apps update {app_name} --source-code-path .")
        else:
            print(f"🆕 Creating new app: {app_name}")
            success = run_command(f"databricks apps create {app_name} --source-code-path .")
        
        if success:
            print("✅ App deployed successfully!")
            print(f"🌐 Access your app at: https://<your-workspace>.databricks.com/apps/{app_name}")
            return True
        else:
            print("❌ Deployment failed")
            return False
            
    finally:
        os.chdir(original_cwd)

def create_workspace_config():
    """Create workspace configuration example"""
    config_content = {
        "app_name": "databricks-job-approval-workflow",
        "display_name": "Job Approval Workflow",
        "description": "Streamlit app for Databricks job approval workflows",
        "unity_catalog": {
            "catalog": "main",
            "schema": "approval_workflow"
        },
        "permissions": {
            "required_permissions": [
                "jobs:read",
                "jobs:run", 
                "sql:read",
                "sql:write"
            ],
            "user_groups": {
                "admins": ["admin-group"],
                "approvers": ["approver-group", "manager-group"],
                "requesters": ["all-users"]
            }
        },
        "job_configuration": {
            "default_job_1": "72987522543057",
            "default_job_2": "883505719297722"
        }
    }
    
    with open("databricks_workspace_config.json", "w") as f:
        json.dump(config_content, f, indent=2)
    
    print("✅ Created databricks_workspace_config.json")

def main():
    """Main deployment workflow"""
    print("🚀 Databricks Apps Deployment Script")
    print("=" * 50)
    
    # Validate environment
    if not check_databricks_cli():
        print("❌ Please configure Databricks CLI first")
        sys.exit(1)
    
    # Validate files
    if not validate_app_files():
        print("❌ Please ensure all required files are present")
        sys.exit(1)
    
    # Setup Unity Catalog
    setup_unity_catalog()
    
    # Create deployment bundle
    bundle_dir = create_app_bundle()
    
    # Create workspace configuration
    create_workspace_config()
    
    # Deploy to Databricks Apps
    if deploy_to_databricks_apps(bundle_dir):
        print("\n🎉 Deployment completed successfully!")
        print("\n📋 Next steps:")
        print("1. Configure Unity Catalog permissions")
        print("2. Set up user groups and roles")
        print("3. Test the application with your jobs")
        print("4. Review databricks_workspace_config.json for customization")
    else:
        print("\n❌ Deployment failed. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
