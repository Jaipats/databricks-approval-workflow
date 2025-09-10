# Databricks Job Approval Workflow App

A comprehensive Streamlit application for managing Databricks job execution with approval workflows. This app allows users to trigger jobs that require approval before executing subsequent dependent jobs, with full state management in Databricks Lakehouse.

## 🚀 Features

- **Job Triggering**: Trigger two dependent Databricks jobs with approval workflow
- **Approval Process**: Built-in approval system with user roles and permissions  
- **State Management**: All approval states stored in Databricks Delta tables
- **Real-time Monitoring**: Track job status and approval workflow progress
- **User Management**: Role-based access control (requester, approver, admin)
- **Audit Trail**: Complete audit log of all actions and state changes
- **Notifications**: Configurable notifications (console, Slack, email)

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Streamlit UI  │────│  Backend APIs   │────│ Databricks Jobs │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │ Delta Tables    │
                    │ (Lakehouse)     │
                    └─────────────────┘
```

## 📋 Workflow Process

1. **Job 1 Trigger**: User triggers the first job through the UI
2. **Job 1 Execution**: First job runs and completes
3. **Approval Required**: System creates approval request in Delta table
4. **Approval Process**: Authorized users can approve or reject the request  
5. **Job 2 Trigger**: Upon approval, second job is automatically triggered
6. **Completion**: Workflow marked as completed when Job 2 finishes

## 🛠️ Installation & Setup

### Prerequisites

- Python 3.8+
- Access to a Databricks workspace
- Databricks personal access token
- Git

### Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd databricks-approval-app
   ```

2. **Run setup script**
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your Databricks credentials
   ```

4. **Start the application**
   ```bash
   ./run.sh
   ```

5. **Open in browser**
   ```
   http://localhost:8501
   ```

### Manual Installation

1. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\\Scripts\\activate
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-access-token"
   ```

4. **Run the application**
   ```bash
   streamlit run app.py
   ```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | Required |
| `DATABRICKS_TOKEN` | Databricks access token | Required |
| `CATALOG_NAME` | Lakehouse catalog name | `main` |
| `SCHEMA_NAME` | Schema for approval tables | `approval_workflow` |
| `APPROVAL_TIMEOUT_HOURS` | Hours before approval expires | `72` |
| `ENABLE_NOTIFICATIONS` | Enable notification system | `true` |
| `NOTIFICATION_HANDLERS` | Notification methods | `console` |

### User Roles

- **Requester**: Can create job trigger requests
- **Approver**: Can approve/reject requests and create requests  
- **Admin**: Full access to all features and admin panel

## 📊 Database Schema

The app creates two main Delta tables:

### approval_requests
Stores all approval workflow requests and their current state.

| Column | Type | Description |
|--------|------|-------------|
| request_id | String | Unique request identifier |
| requester_id | String | User who created the request |
| approver_id | String | User who approved/rejected |
| job1_id | String | First job ID |
| job1_run_id | String | First job run ID |
| job2_id | String | Second job ID |  
| job2_run_id | String | Second job run ID |
| status | String | pending/approved/rejected/completed |
| created_at | Timestamp | Request creation time |
| approved_at | Timestamp | Approval time |

### job_audit_log
Complete audit trail of all job and approval actions.

| Column | Type | Description |
|--------|------|-------------|
| log_id | String | Unique log entry ID |
| request_id | String | Associated request ID |
| job_id | String | Job ID |
| action | String | Action performed |
| user_id | String | User who performed action |
| timestamp | Timestamp | When action occurred |

## 🎯 Usage Examples

### Basic Workflow

1. **Navigate to "Job Triggers"**
2. **Enter job configurations**:
   - Job 1 ID: `12345`
   - Job 1 Parameters: `{"input_path": "/data/raw"}`
   - Job 2 ID: `67890`
   - Job 2 Parameters: `{"model_path": "/models/latest"}`
3. **Click "Trigger First Job"**
4. **Monitor in "Approval Dashboard"** (as approver)
5. **Approve request** once Job 1 completes
6. **Track completion** in "Job History"

### API Integration

The backend provides REST-like interfaces that can be integrated:

```python
# Example: Create approval request programmatically
from backend.approval_manager import ApprovalManager
from backend.database_manager import DatabaseManager

approval_mgr = ApprovalManager()
db_mgr = DatabaseManager()

# Create request
request_id, approval_data = approval_mgr.create_approval_request(
    requester_id="user@company.com",
    job1_config={"job_id": "123", "params": {"key": "value"}},
    job2_config={"job_id": "456", "params": {"key": "value"}}
)

# Store in database
db_mgr.create_approval_request(approval_data)
```

## 🔧 Development

### Project Structure
```
databricks-approval-app/
├── app.py                 # Main Streamlit application
├── backend/              # Backend modules
│   ├── job_manager.py    # Databricks Jobs API integration  
│   ├── approval_manager.py # Approval workflow logic
│   └── database_manager.py # Delta table operations
├── config/               # Configuration files
├── deploy/              # Deployment scripts
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

### Testing

Run with mock jobs for development:
```bash
export USE_MOCK_JOBS=true
streamlit run app.py
```

### Adding New Features

1. **Backend Logic**: Add to appropriate manager class
2. **UI Components**: Update `app.py` with new pages/components
3. **Database Changes**: Update schema in `database_manager.py`
4. **Configuration**: Add new settings to `app_config.py`

## 📱 Databricks App Deployment

To deploy as a Databricks App:

1. **Upload code** to Databricks workspace
2. **Configure environment** variables in Databricks
3. **Create Databricks App** using the deployment configuration
4. **Set up Delta tables** in your Databricks catalog

See `deploy/databricks_config.py` for detailed deployment configuration.

## 🔒 Security Considerations

- **Access Tokens**: Store Databricks tokens securely
- **Role Validation**: Implement proper user role verification  
- **Audit Trail**: All actions are logged for compliance
- **Session Management**: Configure appropriate session timeouts

## 🐛 Troubleshooting

### Common Issues

1. **Connection Failed**: Check Databricks host URL and token
2. **Permission Denied**: Verify token has necessary permissions
3. **Table Not Found**: Ensure catalog and schema exist
4. **Job Not Found**: Verify job IDs exist in workspace

### Debug Mode

Enable debug mode for additional logging:
```bash
export DEBUG_MODE=true
export LOG_LEVEL=DEBUG
streamlit run app.py
```

## 📞 Support

For issues and questions:

1. Check the troubleshooting section
2. Review Databricks logs in the workspace  
3. Enable debug mode for detailed error messages
4. Check the audit log table for workflow state

## 🔮 Future Enhancements

- [ ] Multi-step approval workflows
- [ ] Integration with external approval systems
- [ ] Advanced scheduling capabilities  
- [ ] Mobile-responsive design improvements
- [ ] Integration with Databricks Unity Catalog permissions
- [ ] Webhook support for external integrations
- [ ] Advanced analytics and reporting dashboard

## 📄 License

This project is provided as-is for demonstration and educational purposes.

---

**Built with ❤️ for the Databricks community**
