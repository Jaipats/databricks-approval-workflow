"""
Databricks Job Approval Workflow App - SDK Version
Enhanced Streamlit application using Databricks SDK
"""

import streamlit as st
import os
from datetime import datetime
import pandas as pd
import json
from typing import Dict, List, Optional
import uuid

# Page configuration
st.set_page_config(
    page_title="Databricks Job Approval Workflow",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import custom modules
try:
    from backend.sdk_job_manager import SDKJobManager, MockSDKJobManager
    from backend.approval_manager import ApprovalManager
    from backend.database_manager import DatabaseManager
except ImportError as e:
    st.error(f"Failed to import backend modules: {str(e)}")
    st.stop()

# Configuration
USE_MOCK = os.getenv("USE_MOCK_JOBS", "false").lower() == "true"
DATABRICKS_PROFILE = os.getenv("DATABRICKS_PROFILE", "DEFAULT")

def initialize_managers():
    """Initialize all manager classes"""
    try:
        # Initialize job manager
        if USE_MOCK:
            job_manager = MockSDKJobManager()
            st.sidebar.info("🧪 Running in mock mode")
        else:
            job_manager = SDKJobManager(profile=DATABRICKS_PROFILE)
        
        # Initialize other managers
        approval_manager = ApprovalManager()
        db_manager = DatabaseManager()
        
        # Initialize database tables
        db_manager.initialize_tables()
        
        return job_manager, approval_manager, db_manager
        
    except Exception as e:
        st.error(f"Failed to initialize managers: {str(e)}")
        st.info("💡 Try setting USE_MOCK_JOBS=true in environment variables for testing")
        st.stop()

def main():
    """Main application function"""
    
    # Initialize managers
    job_manager, approval_manager, db_manager = initialize_managers()
    
    # Sidebar navigation
    st.sidebar.title("🚀 Job Approval Workflow")
    page = st.sidebar.selectbox(
        "Navigate to:",
        ["Job Triggers", "Approval Dashboard", "Job History", "Admin Panel"]
    )
    
    # User authentication (simplified)
    if "user_id" not in st.session_state:
        st.session_state.user_id = st.sidebar.text_input("Enter User ID:", value="user_001", key="user_input")
    
    if "user_role" not in st.session_state:
        st.session_state.user_role = st.sidebar.selectbox("Role:", ["requester", "approver", "admin"], key="role_select")
    
    # Connection status
    connection_status = job_manager.validate_connection()
    if connection_status:
        st.sidebar.success("✅ Connected to Databricks")
    else:
        st.sidebar.error("❌ Databricks connection failed")
    
    # Main content based on page selection
    if page == "Job Triggers":
        render_job_triggers_page(job_manager, approval_manager, db_manager)
    elif page == "Approval Dashboard":
        render_approval_dashboard(approval_manager, db_manager, job_manager)
    elif page == "Job History":
        render_job_history(db_manager)
    elif page == "Admin Panel":
        render_admin_panel(db_manager, job_manager)

def render_job_triggers_page(job_manager, approval_manager, db_manager):
    """Render the job triggers page with dropdown selections"""
    st.header("🎯 Trigger Databricks Jobs with Approval Workflow")
    
    # Load available jobs
    with st.spinner("Loading available jobs..."):
        available_jobs = job_manager.list_jobs(limit=100)
    
    if not available_jobs:
        st.error("❌ No jobs found or failed to connect to Databricks")
        return
    
    # Create job options for dropdowns
    job_options = {f"{job['name']} (ID: {job['job_id']})": job['job_id'] for job in available_jobs}
    
    # Predefined job IDs from user request
    PREDEFINED_JOB1_ID = "72987522543057"
    PREDEFINED_JOB2_ID = "883505719297722"
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📋 Workflow Configuration")
        
        # Job 1 Selection (requires approval)
        st.write("**First Job (Requires Approval After Completion)**")
        
        # Check if predefined job exists in available jobs
        job1_default_index = 0
        for i, (name, job_id) in enumerate(job_options.items()):
            if job_id == PREDEFINED_JOB1_ID:
                job1_default_index = i
                break
        
        job1_selection = st.selectbox(
            "Select First Job:",
            options=list(job_options.keys()),
            index=job1_default_index,
            key="job1_select"
        )
        job1_id = job_options[job1_selection]
        
        # Show job details
        if job1_id:
            job1_details = job_manager.get_job_details(job1_id)
            if job1_details:
                with st.expander("📊 First Job Details"):
                    st.json(job1_details)
        
        # Job 1 Parameters
        job1_params = st.text_area(
            "First Job Parameters (JSON):",
            value='{"notebook_params": {"input_date": "2024-09-09", "environment": "production"}}',
            height=100,
            key="job1_params"
        )
        
        st.divider()
        
        # Job 2 Selection (triggered after approval)
        st.write("**Second Job (Auto-triggered After Approval)**")
        
        job2_default_index = 0
        for i, (name, job_id) in enumerate(job_options.items()):
            if job_id == PREDEFINED_JOB2_ID:
                job2_default_index = i
                break
        
        job2_selection = st.selectbox(
            "Select Second Job:",
            options=list(job_options.keys()),
            index=job2_default_index,
            key="job2_select"
        )
        job2_id = job_options[job2_selection]
        
        # Show job details
        if job2_id:
            job2_details = job_manager.get_job_details(job2_id)
            if job2_details:
                with st.expander("📊 Second Job Details"):
                    st.json(job2_details)
        
        # Job 2 Parameters
        job2_params = st.text_area(
            "Second Job Parameters (JSON):",
            value='{"notebook_params": {"model_name": "approval_workflow_model", "experiment": "/Shared/experiments"}}',
            height=100,
            key="job2_params"
        )
    
    with col2:
        st.subheader("🚀 Workflow Execution")
        
        # Workflow summary
        st.info(f"""
        **Workflow Summary:**
        
        1️⃣ **First Job:** {job1_selection.split('(')[0].strip()}
        - Will run first and require approval
        
        2️⃣ **Second Job:** {job2_selection.split('(')[0].strip()}  
        - Will run after approval is granted
        
        👤 **Requester:** {st.session_state.user_id}
        """)
        
        # Validation
        valid_workflow = job1_id != job2_id and job1_id and job2_id
        
        if not valid_workflow:
            if job1_id == job2_id:
                st.error("⚠️ First and Second jobs must be different")
            else:
                st.error("⚠️ Please select both jobs")
        
        # Trigger button
        if st.button("🎯 Start Approval Workflow", 
                    type="primary", 
                    disabled=not valid_workflow,
                    use_container_width=True):
            
            if valid_workflow:
                try:
                    # Validate JSON parameters
                    try:
                        params1 = json.loads(job1_params) if job1_params.strip() else {}
                        params2 = json.loads(job2_params) if job2_params.strip() else {}
                    except json.JSONDecodeError as e:
                        st.error(f"❌ Invalid JSON parameters: {str(e)}")
                        return
                    
                    # Create approval request
                    request_id = str(uuid.uuid4())
                    
                    # Store approval request in database
                    approval_data = {
                        'request_id': request_id,
                        'requester_id': st.session_state.user_id,
                        'job1_id': job1_id,
                        'job1_params': json.dumps(params1),
                        'job2_id': job2_id,
                        'job2_params': json.dumps(params2),
                        'status': 'pending',
                        'created_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat(),
                        'job1_name': job1_selection.split('(')[0].strip(),
                        'job2_name': job2_selection.split('(')[0].strip()
                    }
                    
                    success = db_manager.create_approval_request(approval_data)
                    
                    if not success:
                        st.error("❌ Failed to create approval request in database")
                        return
                    
                    # Trigger first job
                    with st.spinner(f"Triggering first job: {job1_selection.split('(')[0].strip()}..."):
                        job1_run_id = job_manager.trigger_job(job1_id, params1)
                    
                    if job1_run_id:
                        # Update approval request with job1 run ID
                        db_manager.update_approval_request(request_id, {
                            'job1_run_id': job1_run_id,
                            'job1_status': 'running'
                        })
                        
                        st.success(f"✅ First job triggered successfully!")
                        st.info(f"📝 **Request ID:** `{request_id}`")
                        st.info(f"🏃 **Job 1 Run ID:** `{job1_run_id}`")
                        
                        st.balloons()
                        
                        # Show next steps
                        st.info("""
                        **Next Steps:**
                        1. 🔄 Job 1 is now running
                        2. ⏳ Wait for Job 1 to complete
                        3. 👀 Monitor progress in 'Approval Dashboard'
                        4. ✅ Approver will be notified when Job 1 completes
                        5. 🚀 Job 2 will start after approval
                        """)
                        
                        # Auto-refresh after 3 seconds
                        time.sleep(3)
                        st.rerun()
                        
                    else:
                        st.error("❌ Failed to trigger first job")
                        # Clean up failed request
                        db_manager.update_approval_request(request_id, {
                            'status': 'failed',
                            'updated_at': datetime.now().isoformat()
                        })
                        
                except Exception as e:
                    st.error(f"❌ Error starting workflow: {str(e)}")
    
    # Show recent requests for current user
    st.subheader("📋 Your Recent Workflow Requests")
    recent_requests = db_manager.get_user_requests(st.session_state.user_id, limit=5)
    
    if not recent_requests.empty:
        # Format the dataframe for better display
        display_cols = ['request_id', 'status', 'created_at']
        # Add job name columns if they exist
        if 'job1_name' in recent_requests.columns:
            display_cols.insert(1, 'job1_name')
        if 'job2_name' in recent_requests.columns:
            display_cols.insert(2, 'job2_name')
            
        display_df = recent_requests[display_cols].copy()
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
                    options=["pending", "approved", "rejected", "completed"],
                ),
                "created_at": "Created At"
            },
            use_container_width=True
        )
    else:
        st.info("No recent requests found.")

def render_approval_dashboard(approval_manager, db_manager, job_manager):
    """Render the approval dashboard with improved forms"""
    st.header("✅ Approval Dashboard")
    
    if st.session_state.user_role not in ["approver", "admin"]:
        st.warning("⚠️ You need approver or admin role to access this page.")
        return
    
    # Get pending approvals
    with st.spinner("Loading pending approvals..."):
        pending_approvals = db_manager.get_pending_approvals()
    
    if pending_approvals.empty:
        st.info("🎉 No pending approvals at this time!")
        return
    
    st.subheader(f"📝 Pending Approvals ({len(pending_approvals)})")
    
    for idx, row in pending_approvals.iterrows():
        with st.expander(f"📋 Request {row['request_id'][:8]}... - {row['requester_id']} - {row.get('job1_name', 'Unknown Job')}"):
            
            col1, col2, col3 = st.columns([1, 1, 1])
            
            with col1:
                st.write("**📊 Request Information**")
                st.write(f"**Requester:** {row['requester_id']}")
                st.write(f"**Created:** {row['created_at']}")
                st.write(f"**Request ID:** `{row['request_id'][:16]}...`")
                
                # Calculate age
                created_time = pd.to_datetime(row['created_at'])
                age_hours = (pd.Timestamp.now() - created_time).total_seconds() / 3600
                st.write(f"**Age:** {age_hours:.1f} hours")
            
            with col2:
                st.write("**🎯 Job Information**")
                st.write(f"**Job 1:** {row.get('job1_name', 'Unknown')} (`{row['job1_id']}`)")
                st.write(f"**Job 2:** {row.get('job2_name', 'Unknown')} (`{row['job2_id']}`)")
                
                # Check Job 1 status
                job1_run_id = row.get('job1_run_id')
                if job1_run_id:
                    job1_status = job_manager.get_run_status(job1_run_id)
                    status_color = "green" if job1_status == "completed" else "orange" if job1_status == "running" else "red"
                    st.write(f"**Job 1 Status:** :{status_color}[{job1_status}]")
                    st.write(f"**Job 1 Run ID:** `{job1_run_id}`")
                else:
                    st.write("**Job 1 Status:** :gray[Not started]")
                
                # Show parameters
                with st.expander("View Parameters"):
                    st.write("**Job 1 Parameters:**")
                    st.code(row['job1_params'], language='json')
                    st.write("**Job 2 Parameters:**")
                    st.code(row['job2_params'], language='json')
            
            with col3:
                st.write("**⚡ Actions**")
                
                # Only show approval actions if Job 1 is completed successfully
                job1_completed = row.get('job1_status') == 'completed' or (
                    job1_run_id and job_manager.get_run_status(job1_run_id) == 'completed'
                )
                
                if job1_completed:
                    st.success("✅ Job 1 completed successfully!")
                    
                    # Approval form
                    with st.form(key=f"approval_form_{row['request_id']}"):
                        st.write("**Approval Decision:**")
                        
                        approval_comments = st.text_area(
                            "Comments (optional):",
                            placeholder="Add any comments about your decision...",
                            key=f"comments_{row['request_id']}"
                        )
                        
                        col_approve, col_reject = st.columns(2)
                        
                        with col_approve:
                            approve_clicked = st.form_submit_button(
                                "✅ Approve",
                                type="primary",
                                use_container_width=True
                            )
                        
                        with col_reject:
                            reject_clicked = st.form_submit_button(
                                "❌ Reject",
                                use_container_width=True
                            )
                        
                        if approve_clicked:
                            # Update approval status
                            db_manager.update_approval_request(row['request_id'], {
                                'status': 'approved',
                                'approver_id': st.session_state.user_id,
                                'approved_at': datetime.now().isoformat(),
                                'approval_comments': approval_comments,
                                'updated_at': datetime.now().isoformat()
                            })
                            
                            # Trigger Job 2
                            with st.spinner("Triggering second job..."):
                                try:
                                    job2_params = json.loads(row['job2_params'])
                                    job2_run_id = job_manager.trigger_job(row['job2_id'], job2_params)
                                    
                                    if job2_run_id:
                                        db_manager.update_approval_request(row['request_id'], {
                                            'job2_run_id': job2_run_id,
                                            'job2_status': 'running'
                                        })
                                        st.success(f"✅ Approved! Job 2 triggered with Run ID: `{job2_run_id}`")
                                    else:
                                        st.error("❌ Approval recorded but failed to trigger Job 2")
                                        
                                except Exception as e:
                                    st.error(f"❌ Error triggering Job 2: {str(e)}")
                            
                            time.sleep(2)
                            st.rerun()
                        
                        if reject_clicked:
                            if not approval_comments.strip():
                                st.error("⚠️ Please provide a reason for rejection")
                            else:
                                db_manager.update_approval_request(row['request_id'], {
                                    'status': 'rejected',
                                    'approver_id': st.session_state.user_id,
                                    'rejected_at': datetime.now().isoformat(),
                                    'rejection_reason': approval_comments,
                                    'updated_at': datetime.now().isoformat()
                                })
                                st.error("❌ Request rejected")
                                time.sleep(2)
                                st.rerun()
                
                else:
                    if job1_run_id:
                        current_status = job_manager.get_run_status(job1_run_id)
                        if current_status == 'running':
                            st.info("⏳ Waiting for Job 1 to complete...")
                            if st.button(f"🔄 Refresh Status", key=f"refresh_{row['request_id']}"):
                                st.rerun()
                        elif current_status == 'failed':
                            st.error("❌ Job 1 failed - cannot approve")
                        else:
                            st.info(f"ℹ️ Job 1 status: {current_status}")
                    else:
                        st.info("⏳ Job 1 not started yet")

def render_job_history(db_manager):
    """Render job history page with enhanced filtering"""
    st.header("📊 Workflow History")
    
    # Filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        status_filter = st.selectbox("Status:", ["All", "pending", "approved", "rejected", "completed"])
    
    with col2:
        user_filter = st.text_input("User ID Filter:")
    
    with col3:
        days_filter = st.selectbox("Last N days:", [7, 30, 90, 365])
    
    with col4:
        if st.button("🔄 Refresh"):
            st.rerun()
    
    # Get filtered history
    with st.spinner("Loading history..."):
        history = db_manager.get_request_history(
            status=None if status_filter == "All" else status_filter,
            user_id=user_filter if user_filter else None,
            days=days_filter
        )
    
    if not history.empty:
        # Process data for better display
        display_history = history.copy()
        display_history['request_id'] = display_history['request_id'].str[:8] + '...'
        display_history['created_at'] = pd.to_datetime(display_history['created_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Select relevant columns
        display_columns = ['request_id', 'requester_id', 'job1_name', 'job2_name', 'status', 'created_at', 'approver_id']
        display_history = display_history[display_columns]
        
        st.dataframe(
            display_history, 
            use_container_width=True,
            column_config={
                "request_id": "Request ID",
                "requester_id": "Requester",
                "job1_name": "First Job",
                "job2_name": "Second Job",
                "status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["pending", "approved", "rejected", "completed"],
                ),
                "created_at": "Created At",
                "approver_id": "Approver"
            }
        )
        
        # Summary statistics
        st.subheader("📈 Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Requests", len(history))
        with col2:
            approved_count = len(history[history['status'] == 'approved'])
            st.metric("Approved", approved_count)
        with col3:
            rejected_count = len(history[history['status'] == 'rejected'])
            st.metric("Rejected", rejected_count)
        with col4:
            pending_count = len(history[history['status'] == 'pending'])
            st.metric("Pending", pending_count)
        
        # Success rate
        if len(history) > 0:
            completed_count = len(history[history['status'] == 'completed'])
            success_rate = (completed_count / len(history)) * 100
            st.metric("Success Rate", f"{success_rate:.1f}%")
    
    else:
        st.info("No history found for the selected filters.")

def render_admin_panel(db_manager, job_manager):
    """Render admin panel with system management tools"""
    st.header("⚙️ Admin Panel")
    
    if st.session_state.user_role != "admin":
        st.warning("⚠️ Admin access required.")
        return
    
    tab1, tab2, tab3, tab4 = st.tabs(["Database Management", "Job Management", "System Health", "Configuration"])
    
    with tab1:
        st.subheader("🗄️ Database Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Refresh Tables"):
                with st.spinner("Refreshing database tables..."):
                    db_manager.initialize_tables()
                st.success("✅ Tables refreshed")
        
        with col2:
            if st.button("🗑️ Clear All Data", type="secondary"):
                if st.checkbox("I understand this will delete all data"):
                    with st.spinner("Clearing all data..."):
                        db_manager.clear_all_data()
                    st.success("✅ All data cleared")
        
        # Show table stats
        st.write("**Database Statistics:**")
        stats = db_manager.get_table_stats()
        st.json(stats)
    
    with tab2:
        st.subheader("⚙️ Job Management")
        
        # Test job connectivity
        if st.button("🔍 Test Job API Connection"):
            with st.spinner("Testing connection..."):
                connection_ok = job_manager.validate_connection()
                
                if connection_ok:
                    st.success("✅ Connection successful!")
                    
                    # Show available jobs
                    jobs = job_manager.list_jobs(limit=10)
                    if jobs:
                        st.write(f"Found {len(jobs)} jobs:")
                        job_df = pd.DataFrame(jobs)
                        st.dataframe(job_df[['job_id', 'name', 'creator_user_name']], use_container_width=True)
                    else:
                        st.warning("⚠️ Connected but no jobs found")
                else:
                    st.error("❌ Connection failed")
        
        # Job details lookup
        st.write("**Job Details Lookup:**")
        job_id_lookup = st.text_input("Enter Job ID:", key="admin_job_lookup")
        
        if job_id_lookup and st.button("🔍 Get Job Details"):
            with st.spinner(f"Getting details for job {job_id_lookup}..."):
                details = job_manager.get_job_details(job_id_lookup)
                if details:
                    st.json(details)
                else:
                    st.error(f"Job {job_id_lookup} not found")
    
    with tab3:
        st.subheader("🏥 System Health")
        
        # System metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Active Sessions", 1)  # Simplified
            st.metric("Database Status", "Healthy")
        
        with col2:
            connection_status = job_manager.validate_connection()
            st.metric("API Status", "Connected" if connection_status else "Disconnected")
            st.metric("Profile", DATABRICKS_PROFILE)
        
        with col3:
            st.metric("Mock Mode", "Yes" if USE_MOCK else "No")
            st.metric("Last Updated", datetime.now().strftime("%H:%M:%S"))
        
        # Recent activity
        if st.button("📊 Show Recent Activity"):
            recent_requests = db_manager.get_request_history(days=7)
            if not recent_requests.empty:
                st.write("**Recent Activity (Last 7 days):**")
                display_cols = ['request_id', 'requester_id', 'status', 'created_at']
                available_cols = [col for col in display_cols if col in recent_requests.columns]
                st.dataframe(recent_requests[available_cols].head(10))
            else:
                st.info("No recent activity")
    
    with tab4:
        st.subheader("🔧 Configuration")
        
        st.write("**Current Configuration:**")
        config_info = {
            "Databricks Profile": DATABRICKS_PROFILE,
            "Mock Mode": USE_MOCK,
            "Database Catalog": "main",
            "Database Schema": "approval_workflow"
        }
        st.json(config_info)
        
        st.write("**Environment Variables:**")
        env_vars = {
            "USE_MOCK_JOBS": os.getenv("USE_MOCK_JOBS", "false"),
            "DATABRICKS_PROFILE": os.getenv("DATABRICKS_PROFILE", "DEFAULT")
        }
        st.json(env_vars)

if __name__ == "__main__":
    # Import time for sleep functionality
    import time
    main()
