"""
Databricks Job Approval Workflow - Databricks Apps Version
Optimized for native deployment in Databricks Apps environment
"""

import streamlit as st
import pandas as pd
import json
import uuid
from datetime import datetime
import time

# Databricks-specific imports
try:
    from pyspark.sql import SparkSession
    from databricks.sdk.runtime import *
    import databricks.sdk.service.iam as iam
    DATABRICKS_RUNTIME = True
except ImportError:
    DATABRICKS_RUNTIME = False

# Import modules
from backend.job_manager import DatabricksJobManager
from backend.storage import DatabricksStorage

# Page configuration
st.set_page_config(
    page_title="Databricks Job Approval Workflow",
    page_icon="🚀",
    layout="wide"
)

def get_current_user():
    """Get current user from Databricks context"""
    try:
        if DATABRICKS_RUNTIME:
            # Get user from Databricks context
            spark = SparkSession.getActiveSession()
            if spark:
                user = spark.sql("SELECT current_user() as user").collect()[0]['user']
                return user
        
        # Fallback for development
        return st.sidebar.text_input("User ID:", value="user@company.com")
    except:
        return st.sidebar.text_input("User ID:", value="user@company.com")

def get_user_role(user_email: str) -> str:
    """Determine user role based on email or group membership"""
    try:
        # In production, integrate with Databricks groups/permissions
        if user_email.endswith("@databricks.com") or "admin" in user_email.lower():
            return "admin"
        elif "approver" in user_email.lower() or "manager" in user_email.lower():
            return "approver"
        else:
            return "requester"
    except:
        return "requester"

@st.cache_resource
def initialize_app():
    """Initialize application components"""
    try:
        # Initialize job manager (uses Databricks runtime context)
        job_manager = DatabricksJobManager()
        
        # Initialize Databricks storage
        storage = DatabricksStorage()
        storage.initialize()
        
        return job_manager, storage
        
    except Exception as e:
        st.error(f"❌ Failed to initialize: {str(e)}")
        st.info("💡 Ensure you're running in Databricks Apps environment")
        st.stop()

def main():
    """Main application"""
    
    # Initialize
    job_manager, storage = initialize_app()
    
    # Get user context
    current_user = get_current_user()
    user_role = get_user_role(current_user)
    
    # Sidebar
    st.sidebar.title("🚀 Job Approval Workflow")
    st.sidebar.success(f"👤 **User:** {current_user}")
    st.sidebar.info(f"🔑 **Role:** {user_role.title()}")
    
    # Navigation
    page = st.sidebar.selectbox("Navigate:", [
        "🎯 Trigger Jobs", 
        "✅ Approvals", 
        "📊 History", 
        "⚙️ Admin" if user_role == "admin" else None
    ], format_func=lambda x: x if x else "")
    
    # Connection status
    if job_manager.validate_connection():
        st.sidebar.success("✅ Connected to Databricks")
    else:
        st.sidebar.error("❌ Connection failed")
    
    # Route to pages
    if page == "🎯 Trigger Jobs":
        trigger_jobs_page(job_manager, storage, current_user, user_role)
    elif page == "✅ Approvals":
        approvals_page(job_manager, storage, current_user, user_role)
    elif page == "📊 History":
        history_page(storage, current_user, user_role)
    elif page == "⚙️ Admin":
        admin_page(storage, current_user, user_role)

def trigger_jobs_page(job_manager, storage, user_id: str, user_role: str):
    """Job triggering page"""
    st.header("🎯 Trigger Jobs with Approval Workflow")
    
    # Load jobs
    with st.spinner("🔍 Loading jobs from workspace..."):
        jobs = job_manager.list_jobs(limit=100)
    
    if not jobs:
        st.error("❌ No jobs found in workspace")
        return
    
    st.success(f"✅ Found {len(jobs)} jobs in workspace")
    
    # Predefined job IDs
    PREDEFINED_JOB1 = "72987522543057"  # BeforeApproval
    PREDEFINED_JOB2 = "883505719297722"  # AfterApproval
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📋 Workflow Configuration")
        
        # Create job lookup
        job_lookup = {job['job_id']: job for job in jobs}
        job_options = [f"{job['name']} (ID: {job['job_id']})" for job in jobs]
        
        # Job 1 selection
        st.write("**🥇 First Job (Requires Approval)**")
        
        # Find default job 1
        job1_default = 0
        for i, job in enumerate(jobs):
            if job['job_id'] == PREDEFINED_JOB1:
                job1_default = i
                break
        
        job1_selection = st.selectbox("Select First Job:", job_options, index=job1_default, key="job1")
        job1_id = job1_selection.split("(ID: ")[1].rstrip(")")
        job1_name = job1_selection.split(" (ID:")[0]
        
        # Show job 1 details
        if st.expander(f"📋 {job1_name} Details"):
            job1_details = job_manager.get_job_details(job1_id)
            if job1_details:
                st.json(job1_details)
        
        job1_params = st.text_area(
            "First Job Parameters (JSON):",
            value='{"notebook_params": {"input_date": "2024-09-10", "environment": "prod"}}',
            height=80
        )
        
        st.divider()
        
        # Job 2 selection
        st.write("**🥈 Second Job (Triggered After Approval)**")
        
        job2_default = 1
        for i, job in enumerate(jobs):
            if job['job_id'] == PREDEFINED_JOB2:
                job2_default = i
                break
        
        job2_selection = st.selectbox("Select Second Job:", job_options, index=job2_default, key="job2")
        job2_id = job2_selection.split("(ID: ")[1].rstrip(")")
        job2_name = job2_selection.split(" (ID:")[0]
        
        # Show job 2 details
        if st.expander(f"📋 {job2_name} Details"):
            job2_details = job_manager.get_job_details(job2_id)
            if job2_details:
                st.json(job2_details)
        
        job2_params = st.text_area(
            "Second Job Parameters (JSON):",
            value='{"notebook_params": {"model_name": "approval_workflow_model", "data_source": "processed"}}',
            height=80
        )
    
    with col2:
        st.subheader("🚀 Execute Workflow")
        
        # Workflow summary
        st.info(f"""
        **📋 Workflow Summary:**
        
        **👤 User:** `{user_id}`
        **🔑 Role:** `{user_role}`
        
        **1️⃣ First Job:**
        `{job1_name}`
        - Will execute immediately
        - Requires approval when complete
        
        **2️⃣ Second Job:**
        `{job2_name}`
        - Will execute after approval
        """)
        
        # Validation
        valid_workflow = job1_id != job2_id and job1_id and job2_id
        
        if not valid_workflow:
            st.error("⚠️ Jobs must be different")
        
        # Trigger button
        if st.button("🎯 Start Approval Workflow", type="primary", disabled=not valid_workflow, use_container_width=True):
            try:
                # Validate parameters
                params1 = json.loads(job1_params) if job1_params.strip() else {}
                params2 = json.loads(job2_params) if job2_params.strip() else {}
                
                # Create request
                request_id = str(uuid.uuid4())
                
                request_data = {
                    'request_id': request_id,
                    'requester_id': user_id,
                    'job1_id': job1_id,
                    'job1_name': job1_name,
                    'job1_params': json.dumps(params1),
                    'job2_id': job2_id,
                    'job2_name': job2_name,
                    'job2_params': json.dumps(params2),
                    'status': 'pending',
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat(),
                }
                
                # Save request to Unity Catalog
                if storage.create_request(request_data):
                    # Trigger first job
                    with st.spinner(f"🔄 Triggering {job1_name}..."):
                        job1_run_id = job_manager.trigger_job(job1_id, params1)
                    
                    if job1_run_id:
                        # Update with run ID
                        storage.update_request(request_id, {
                            'job1_run_id': job1_run_id,
                            'job1_status': 'running'
                        })
                        
                        st.success("✅ Workflow started successfully!")
                        st.info(f"**Request ID:** `{request_id}`")
                        st.info(f"**Job 1 Run ID:** `{job1_run_id}`")
                        
                        st.balloons()
                        
                        # Next steps
                        st.success("""
                        **🎉 Next Steps:**
                        1. **Job 1** is now running in your workspace
                        2. Monitor progress in **Approvals** page
                        3. **Approvers** will be notified when Job 1 completes
                        4. **Job 2** will start automatically after approval
                        """)
                        
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.error("❌ Failed to trigger first job")
                else:
                    st.error("❌ Failed to create workflow request")
                    
            except json.JSONDecodeError as e:
                st.error(f"❌ Invalid JSON parameters: {str(e)}")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # Recent requests
    st.subheader("📋 Your Recent Workflow Requests")
    recent = storage.get_user_requests(user_id, limit=5)
    
    if not recent.empty:
        # Format display
        display_df = recent[['request_id', 'job1_name', 'job2_name', 'status', 'created_at']].copy()
        display_df['request_id'] = display_df['request_id'].str[:8] + '...'
        display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        st.dataframe(
            display_df,
            column_config={
                "request_id": "Request ID",
                "job1_name": "First Job",
                "job2_name": "Second Job",
                "status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["pending", "approved", "rejected", "completed"]
                ),
                "created_at": "Created"
            },
            use_container_width=True
        )
    else:
        st.info("No recent requests found")

def approvals_page(job_manager, storage, user_id: str, user_role: str):
    """Approval dashboard"""
    st.header("✅ Approval Dashboard")
    
    if user_role not in ["approver", "admin"]:
        st.warning("⚠️ You need **approver** or **admin** role to access this page")
        st.info("Contact your administrator for role assignment")
        return
    
    # Get pending approvals
    with st.spinner("🔍 Loading pending approvals..."):
        pending = storage.get_pending_requests()
    
    if pending.empty:
        st.success("🎉 No pending approvals at this time!")
        st.info("New approval requests will appear here automatically")
        return
    
    st.subheader(f"📝 Pending Approvals ({len(pending)})")
    
    for _, row in pending.iterrows():
        with st.expander(f"📋 **{row.get('job1_name', 'Unknown Job')}** → {row.get('job2_name', 'Unknown Job')} | Requested by: **{row['requester_id']}**", expanded=True):
            
            col1, col2, col3 = st.columns([1, 1, 1])
            
            with col1:
                st.write("**📋 Request Details**")
                st.write(f"**ID:** `{row['request_id'][:12]}...`")
                st.write(f"**Requester:** {row['requester_id']}")
                st.write(f"**Created:** {row['created_at']}")
                
                # Calculate age
                try:
                    created = pd.to_datetime(row['created_at'])
                    age_hours = (pd.Timestamp.now() - created).total_seconds() / 3600
                    st.write(f"**Age:** {age_hours:.1f} hours")
                except:
                    pass
            
            with col2:
                st.write("**🎯 Job Information**")
                st.write(f"**Job 1:** {row.get('job1_name', 'Unknown')} (`{row['job1_id']}`)")
                st.write(f"**Job 2:** {row.get('job2_name', 'Unknown')} (`{row['job2_id']}`)")
                
                # Check job 1 status
                job1_run_id = row.get('job1_run_id')
                if job1_run_id:
                    job1_status = job_manager.get_run_status(job1_run_id)
                    if job1_status == "completed":
                        st.success(f"**Job 1 Status:** ✅ {job1_status}")
                    elif job1_status == "running":
                        st.info(f"**Job 1 Status:** 🔄 {job1_status}")
                    else:
                        st.error(f"**Job 1 Status:** ❌ {job1_status}")
                    st.write(f"**Run ID:** `{job1_run_id}`")
                else:
                    st.write("**Job 1 Status:** ⏳ Not started")
                
                # Show parameters
                with st.expander("📋 View Parameters"):
                    st.write("**Job 1 Parameters:**")
                    st.code(row['job1_params'], language='json')
                    st.write("**Job 2 Parameters:**")
                    st.code(row['job2_params'], language='json')
            
            with col3:
                st.write("**⚡ Approval Actions**")
                
                # Check if Job 1 is completed
                job1_completed = False
                if job1_run_id:
                    current_status = job_manager.get_run_status(job1_run_id)
                    job1_completed = current_status == 'completed'
                
                if job1_completed:
                    st.success("✅ **Job 1 Completed Successfully!**")
                    
                    # Approval form
                    with st.form(key=f"approval_{row['request_id']}"):
                        st.write("**Make Approval Decision:**")
                        
                        approval_comments = st.text_area(
                            "Comments:",
                            placeholder="Add optional comments about your decision...",
                            height=80
                        )
                        
                        col_approve, col_reject = st.columns(2)
                        
                        with col_approve:
                            approved = st.form_submit_button("✅ **Approve**", type="primary", use_container_width=True)
                        
                        with col_reject:
                            rejected = st.form_submit_button("❌ **Reject**", use_container_width=True)
                        
                        if approved:
                            # Approve and trigger Job 2
                            storage.update_request(row['request_id'], {
                                'status': 'approved',
                                'approver_id': user_id,
                                'approved_at': datetime.now().isoformat(),
                                'approval_comments': approval_comments or ''
                            })
                            
                            # Trigger Job 2
                            with st.spinner("🔄 Triggering second job..."):
                                try:
                                    params2 = json.loads(row['job2_params'])
                                    job2_run_id = job_manager.trigger_job(row['job2_id'], params2)
                                    
                                    if job2_run_id:
                                        storage.update_request(row['request_id'], {
                                            'job2_run_id': job2_run_id,
                                            'job2_status': 'running'
                                        })
                                        
                                        storage.log_action(
                                            request_id=row['request_id'],
                                            job_id=row['job2_id'],
                                            action="approved_and_triggered",
                                            user_id=user_id,
                                            job_type="job2",
                                            run_id=job2_run_id,
                                            details=f"Approved by {user_id}, Job 2 triggered"
                                        )
                                        
                                        st.success(f"🎉 **Approved!** Job 2 triggered: `{job2_run_id}`")
                                    else:
                                        st.error("❌ Approval recorded but Job 2 trigger failed")
                                        
                                except Exception as e:
                                    st.error(f"❌ Error triggering Job 2: {str(e)}")
                            
                            time.sleep(2)
                            st.rerun()
                        
                        if rejected:
                            if not approval_comments.strip():
                                st.error("⚠️ **Please provide a reason for rejection**")
                            else:
                                storage.update_request(row['request_id'], {
                                    'status': 'rejected',
                                    'approver_id': user_id,
                                    'rejected_at': datetime.now().isoformat(),
                                    'rejection_reason': approval_comments
                                })
                                
                                storage.log_action(
                                    request_id=row['request_id'],
                                    job_id=row['job1_id'],
                                    action="rejected",
                                    user_id=user_id,
                                    details=f"Rejected by {user_id}: {approval_comments}"
                                )
                                
                                st.error("❌ **Request Rejected**")
                                time.sleep(2)
                                st.rerun()
                
                else:
                    # Job 1 not completed yet
                    if job1_run_id:
                        status = job_manager.get_run_status(job1_run_id) or "unknown"
                        if status == "running":
                            st.info("⏳ **Waiting for Job 1 to complete**")
                            if st.button(f"🔄 Refresh Status", key=f"refresh_{row['request_id']}", use_container_width=True):
                                st.rerun()
                        elif status == "failed":
                            st.error("❌ **Job 1 Failed** - Cannot approve")
                        else:
                            st.info(f"ℹ️ Job 1 Status: **{status}**")
                    else:
                        st.info("⏳ **Job 1 not started yet**")

def history_page(storage, user_id: str, user_role: str):
    """History and analytics page"""
    st.header("📊 Workflow History & Analytics")
    
    # Filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        status_filter = st.selectbox("Status:", ["All", "pending", "approved", "rejected", "completed"])
    
    with col2:
        days_filter = st.selectbox("Last N Days:", [7, 30, 90, 365])
    
    with col3:
        user_filter = st.text_input("User Filter:", placeholder="user@company.com")
    
    with col4:
        if st.button("🔄 Refresh Data"):
            st.rerun()
    
    # Get history data
    with st.spinner("📊 Loading workflow history..."):
        history = storage.get_history(
            days=days_filter,
            status=None if status_filter == "All" else status_filter,
            user_id=user_filter if user_filter else None
        )
    
    if not history.empty:
        # Display data table
        st.subheader(f"📋 Workflow History ({len(history)} records)")
        
        # Format for display
        display_cols = ['request_id', 'requester_id', 'job1_name', 'job2_name', 'status', 'created_at', 'approver_id']
        available_cols = [col for col in display_cols if col in history.columns]
        display_df = history[available_cols].copy()
        
        # Format columns
        if 'request_id' in display_df.columns:
            display_df['request_id'] = display_df['request_id'].str[:8] + '...'
        if 'created_at' in display_df.columns:
            display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        st.dataframe(
            display_df,
            column_config={
                "request_id": "Request ID",
                "requester_id": "Requester",
                "job1_name": "First Job",
                "job2_name": "Second Job",
                "status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["pending", "approved", "rejected", "completed"]
                ),
                "created_at": "Created At",
                "approver_id": "Approver"
            },
            use_container_width=True
        )
        
        # Analytics
        st.subheader("📈 Workflow Analytics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Requests", len(history))
        with col2:
            approved = len(history[history['status'] == 'approved'])
            st.metric("✅ Approved", approved)
        with col3:
            rejected = len(history[history['status'] == 'rejected'])
            st.metric("❌ Rejected", rejected)
        with col4:
            pending = len(history[history['status'] == 'pending'])
            st.metric("⏳ Pending", pending)
        
        # Success rate
        if len(history) > 0:
            completed = len(history[history['status'] == 'completed'])
            success_rate = (completed / len(history)) * 100
            st.metric("🎯 Success Rate", f"{success_rate:.1f}%")
        
        # Charts
        if len(history) > 1:
            st.subheader("📊 Visual Analytics")
            
            # Status distribution
            status_counts = history['status'].value_counts()
            st.bar_chart(status_counts)
    
    else:
        st.info("📭 No workflow history found for the selected filters")

def admin_page(storage, user_id: str, user_role: str):
    """Admin panel"""
    st.header("⚙️ System Administration")
    
    if user_role != "admin":
        st.warning("⚠️ **Admin access required**")
        st.info("Contact your system administrator for admin privileges")
        return
    
    tab1, tab2, tab3 = st.tabs(["📊 System Stats", "🗄️ Data Management", "🔧 Configuration"])
    
    with tab1:
        st.subheader("📊 System Statistics")
        
        # Get comprehensive stats
        stats = storage.get_stats()
        
        if "error" not in stats:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("🏗️ Storage", stats.get('storage_mode', 'unknown').title())
                st.metric("📊 Total Requests", stats.get('total_requests', 0))
            
            with col2:
                st.metric("⏳ Pending", stats.get('pending_requests', 0))
                st.metric("✅ Approved", stats.get('approved_requests', 0))
            
            with col3:
                st.metric("🎯 Completed", stats.get('completed_requests', 0))
                st.metric("📝 Audit Entries", stats.get('audit_entries', 0))
            
            # Full stats JSON
            st.subheader("🔍 Detailed Statistics")
            st.json(stats)
        else:
            st.error(f"❌ Error loading stats: {stats['error']}")
    
    with tab2:
        st.subheader("🗄️ Data Management")
        
        st.warning("⚠️ **Dangerous Operations** - Use with caution!")
        
        if st.button("🗑️ Clear All Workflow Data", type="secondary"):
            if st.checkbox("✅ I confirm I want to delete ALL workflow data"):
                with st.spinner("🗑️ Clearing all data..."):
                    storage.clear_all()
                st.success("✅ All workflow data cleared")
                st.info("🔄 Page will refresh in 3 seconds...")
                time.sleep(3)
                st.rerun()
    
    with tab3:
        st.subheader("🔧 System Configuration")
        
        st.write("**Unity Catalog Configuration:**")
        st.code(f"""
        Catalog: {storage.catalog}
        Schema: {storage.schema}
        
        Tables:
        - {storage.approval_table}
        - {storage.audit_table}
        """)
        
        st.write("**Current User Context:**")
        st.json({
            "user_id": user_id,
            "user_role": user_role,
            "timestamp": datetime.now().isoformat()
        })

if __name__ == "__main__":
    main()
