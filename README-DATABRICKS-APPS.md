# Databricks Job Approval Workflow - Databricks Apps Version

🚀 **Native Databricks Apps Deployment** - Optimized for Unity Catalog and Databricks Runtime

## Overview

This version is specifically designed to run as a **Databricks App**, leveraging:
- **Unity Catalog** for state storage (no local files)
- **Databricks Runtime** context for authentication
- **Native Databricks SDK** integration
- **Streamlit** for the web interface
- **Built-in user authentication** via Databricks

## Architecture

```
📁 Databricks Apps Architecture
├── 🎯 databricks_app.py          # Main Streamlit application
├── 🗄️ backend/
│   ├── databricks_storage.py     # Unity Catalog storage manager
│   └── sdk_job_manager.py        # Databricks SDK job manager
├── ⚙️ databricks_app_config.yml   # App configuration
├── 📋 databricks_requirements.txt # Minimal dependencies
└── 📚 README-DATABRICKS-APPS.md  # This guide
```

## Features

### ✅ Native Integration
- **Unity Catalog Tables**: `main.approval_workflow.approval_requests` and `main.approval_workflow.audit_log`
- **Databricks Authentication**: Uses current user context automatically
- **Job Management**: Direct integration with Databricks Jobs API
- **Role-based Access**: Automatic role detection based on user/groups

### 🎯 Workflow Features
- **Job Triggering**: Select and trigger jobs with parameters
- **Approval Process**: Multi-step approval workflow with comments
- **State Persistence**: All state stored in Unity Catalog Delta tables
- **Audit Trail**: Complete audit log of all actions
- **Real-time Updates**: Live status updates and notifications

## Deployment Guide

### Step 1: Prepare Unity Catalog

```sql
-- Create the schema (if not exists)
CREATE SCHEMA IF NOT EXISTS main.approval_workflow;

-- Grant permissions to users/groups
GRANT USE CATALOG ON CATALOG main TO `your-group`;
GRANT USE SCHEMA ON SCHEMA main.approval_workflow TO `your-group`;
GRANT CREATE TABLE ON SCHEMA main.approval_workflow TO `your-group`;
```

### Step 2: Deploy as Databricks App

#### Option A: Using Databricks CLI

```bash
# Install/update Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Deploy the app
databricks apps create databricks-job-approval-workflow \
  --source-code-path ./databricks-approval-app \
  --config-file databricks_app_config.yml
```

#### Option B: Using Databricks UI

1. **Navigate to Apps** in your Databricks workspace
2. **Click "Create App"**
3. **Choose "Upload from Local"**
4. **Select the `databricks-approval-app` folder**
5. **Configure app settings:**
   - **Name**: `databricks-job-approval-workflow`
   - **Entry Point**: `databricks_app.py`
   - **Resources**: 2GB RAM, 1 CPU core (adjust as needed)

#### Option C: Using Repos

1. **Clone this repo** to Databricks Repos
2. **Navigate to Apps** → **Create App** → **From Repo**
3. **Select** the `databricks-approval-app` folder
4. **Configure** according to `databricks_app_config.yml`

### Step 3: Configure Permissions

#### Job Permissions
```sql
-- Grant job execution permissions
GRANT EXECUTE ON JOB your_job_id_1 TO `approval-workflow-app`;
GRANT EXECUTE ON JOB your_job_id_2 TO `approval-workflow-app`;
```

#### Unity Catalog Permissions
```sql
-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE main.approval_workflow.approval_requests TO `approval-workflow-users`;
GRANT SELECT, INSERT ON TABLE main.approval_workflow.audit_log TO `approval-workflow-users`;
```

### Step 4: User Role Configuration

The app automatically determines user roles:

- **🔧 Admin**: Users with `admin` in email or member of admin groups
- **✅ Approver**: Users with `approver`/`manager` in email or member of approver groups  
- **👤 Requester**: All other authenticated users

Customize role logic in `databricks_app.py`:

```python
def get_user_role(user_email: str) -> str:
    # Customize this function based on your organization
    # Integration with Databricks groups:
    # - Check group membership via Databricks SDK
    # - Use Unity Catalog permissions
    # - Custom role mapping logic
```

## Usage Guide

### 🎯 Triggering Jobs

1. **Navigate** to **"Trigger Jobs"** page
2. **Select** your jobs from the dropdown (populated from workspace)
3. **Configure** job parameters (JSON format)
4. **Click** "Start Approval Workflow"
5. **Monitor** progress in the dashboard

### ✅ Approving Workflows

1. **Access** the **"Approvals"** page (requires approver role)
2. **Review** pending approval requests
3. **Check** Job 1 completion status
4. **Add** approval comments
5. **Choose** Approve/Reject
6. **Job 2** triggers automatically on approval

### 📊 Monitoring & Analytics

1. **View** workflow history in **"History"** page
2. **Filter** by status, date range, or user
3. **Analyze** approval patterns and success rates
4. **Export** data for reporting

### ⚙️ Administration

1. **Access** admin panel (admin role required)
2. **View** system statistics and health
3. **Manage** data (clear workflows if needed)
4. **Monitor** Unity Catalog usage

## Configuration

### Environment Variables

The app uses Databricks runtime context by default, but you can override:

```python
# Optional environment variables
DATABRICKS_CATALOG = "main"              # Unity Catalog catalog
DATABRICKS_SCHEMA = "approval_workflow"  # Schema name
USE_MOCK_JOBS = "false"                 # Use mock jobs for testing
```

### Unity Catalog Configuration

Default configuration creates tables in:
- **Catalog**: `main`
- **Schema**: `approval_workflow`
- **Tables**: 
  - `approval_requests` - Main workflow state
  - `audit_log` - Complete audit trail

Modify in `databricks_storage.py` if needed:

```python
class DatabricksStorage:
    def __init__(self, catalog: str = "your_catalog", schema: str = "your_schema"):
        # Customize catalog/schema names
```

## Security & Compliance

### ✅ Data Security
- **Unity Catalog Integration**: Leverages Databricks governance
- **Table-level Security**: Row-level and column-level access controls
- **Audit Logging**: Complete audit trail in Unity Catalog
- **User Authentication**: Native Databricks authentication

### ✅ Access Control
- **Role-based Access**: Different permissions for different user types
- **Job Permissions**: Only authorized users can trigger specific jobs
- **Data Isolation**: Proper separation of workflow data

### ✅ Compliance
- **Audit Trail**: All actions logged with timestamps and user IDs
- **Data Lineage**: Unity Catalog tracks all data changes
- **Retention Policies**: Configurable data retention via Unity Catalog

## Monitoring & Troubleshooting

### Health Checks

Monitor app health via:
1. **Databricks Apps Dashboard** - App status and resource usage
2. **Unity Catalog** - Table sizes and query performance
3. **Jobs API** - Job execution success rates
4. **App Logs** - Streamlit application logs

### Common Issues

#### ❌ "Spark session required"
**Solution**: Ensure running in Databricks Apps environment with Spark runtime

#### ❌ Unity Catalog permissions
**Solution**: Grant proper CREATE TABLE and SELECT/INSERT permissions

#### ❌ Job trigger failures  
**Solution**: Verify job IDs and execution permissions

#### ❌ User role detection
**Solution**: Customize `get_user_role()` function for your organization

### Performance Optimization

1. **Table Optimization**:
   ```sql
   OPTIMIZE main.approval_workflow.approval_requests;
   OPTIMIZE main.approval_workflow.audit_log;
   ```

2. **Resource Allocation**: Adjust app resources based on usage:
   ```yaml
   resources:
     requests:
       memory: "4Gi"  # Increase for heavy usage
       cpu: "2000m"
   ```

3. **Query Performance**: Add indexes if needed:
   ```sql
   -- Add Z-order optimization for frequently filtered columns
   OPTIMIZE main.approval_workflow.approval_requests ZORDER BY (status, created_at);
   ```

## Migration from Local Version

To migrate from the local CSV version:

### Step 1: Export Existing Data
```bash
# From local version directory
python -c "
import pandas as pd
import os
if os.path.exists('local_data'):
    approval_df = pd.read_csv('local_data/approval_requests.csv')
    audit_df = pd.read_csv('local_data/audit_log.csv')
    approval_df.to_json('approval_requests_export.json', orient='records')
    audit_df.to_json('audit_log_export.json', orient='records')
    print('Data exported to JSON files')
"
```

### Step 2: Import to Unity Catalog
```python
# In Databricks notebook or via the app's admin panel
import pandas as pd
from backend.databricks_storage import DatabricksStorage

# Initialize storage
storage = DatabricksStorage()
storage.initialize()

# Load and import data
approval_data = pd.read_json('approval_requests_export.json')
for _, row in approval_data.iterrows():
    storage.create_request(row.to_dict())

print("Migration complete!")
```

## Support & Development

### Development Setup
```bash
# For local development (testing)
pip install -r databricks_requirements.txt
streamlit run databricks_app.py
```

### Contributing
1. **Test locally** using mock mode: `USE_MOCK_JOBS=true`
2. **Follow** Databricks Apps best practices
3. **Update** Unity Catalog schema if needed
4. **Test** with real Databricks jobs before deployment

### Support
- **Documentation**: This README and inline code comments
- **Unity Catalog**: Databricks documentation for table management  
- **Databricks SDK**: Official SDK documentation
- **Databricks Apps**: Platform-specific deployment guides

---

🚀 **Ready to deploy!** Follow the deployment guide above to get your job approval workflow running natively in Databricks Apps.
