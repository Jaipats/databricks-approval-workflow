# Databricks Job Approval Workflow - Simplified Version

A streamlined Streamlit application for managing Databricks job execution with approval workflows. This simplified version has minimal dependencies and no Java/Spark requirements.

## ✨ **Key Improvements**

### 🚀 **Simplified Architecture**
- **6 core dependencies** instead of 20+ 
- **No Java runtime required** - eliminates Spark/Java dependency issues
- **Pure Python implementation** with CSV-based local storage
- **Faster startup** and more reliable operation

### 📦 **Dependencies**
```
streamlit>=1.28.0
pandas>=2.0.0  
requests>=2.31.0
databricks-sdk>=0.12.0
python-dateutil>=2.8.0
uuid
```

## 🎯 **Features**

✅ **Job Triggering** - Trigger two dependent Databricks jobs with approval workflow  
✅ **Approval Process** - Built-in approval system with user roles  
✅ **State Management** - CSV-based storage (no Spark dependency)  
✅ **Real-time Monitoring** - Track job status using Databricks SDK  
✅ **User Roles** - Requester, Approver, Admin access levels  
✅ **Audit Trail** - Complete log of all actions  

## 🚀 **Quick Start**

1. **Setup**
   ```bash
   ./setup.sh
   ```

2. **Run**
   ```bash
   ./run.sh
   ```

3. **Access**
   - Open: http://localhost:8501
   - Your jobs: BeforeApproval (72987522543057) & AfterApproval (883505719297722)

## 🎯 **Usage**

### Trigger Workflow
1. Go to "Trigger Jobs" page
2. Select your BeforeApproval and AfterApproval jobs 
3. Add parameters if needed
4. Click "Start Workflow"

### Approve Requests
1. Switch role to "approver" 
2. Go to "Approvals" page
3. Wait for Job 1 to complete
4. Click "Approve" to trigger Job 2

### Monitor Progress
1. Check "History" page for completed workflows
2. View statistics and audit trail
3. Admin users can manage data

## 🔧 **Technical Details**

- **Database**: Local CSV files in `local_data/` directory
- **Authentication**: Uses your Databricks CLI profile
- **Storage**: `approvals.csv` and `audit.csv` files
- **Jobs API**: Databricks Python SDK integration

## 🎉 **Benefits vs Full Version**

| Feature | Full Version | Simplified |
|---------|--------------|------------|
| Dependencies | 20+ packages | 6 packages |
| Java Runtime | Required | Not needed |
| Startup Time | Slow | Fast |
| Storage | Delta/CSV | CSV only |
| Complexity | High | Low |
| Reliability | Java issues | Stable |

## 🛠️ **Files**

- `app.py` - Main Streamlit application (simplified)
- `backend/simple_database.py` - CSV-based storage
- `backend/sdk_job_manager.py` - Databricks SDK integration
- `requirements.txt` - Minimal dependencies
- `local_data/` - CSV storage directory

## 🚀 **Production Ready**

The simplified version is production-ready and eliminates common issues:
- ✅ No Java/Spark runtime dependencies
- ✅ Faster startup and operation  
- ✅ More reliable error handling
- ✅ Same core functionality
- ✅ Better user experience

Perfect for environments where Java is not available or when you want minimal complexity!
