"""
Simplified Databricks Job Approval Workflow App
Streamlined version with minimal dependencies
"""

import streamlit as st
import pandas as pd
import json
import uuid
from datetime import datetime
import time

# Import simplified modules
from backend.sdk_job_manager import SDKJobManager, MockSDKJobManager
from backend.simple_database import SimpleDatabase

# Configuration
import os
USE_MOCK = os.getenv("USE_MOCK_JOBS", "false").lower() == "true"  
DATABRICKS_PROFILE = os.getenv("DATABRICKS_PROFILE", "DEFAULT")

# Page configuration
st.set_page_config(
    page_title="Databricks Job Approval Workflow",
    page_icon="🚀",
    layout="wide"
)

def initialize_app():
    """Initialize the application"""
    try:
        # Initialize job manager
        if USE_MOCK:
            job_manager = MockSDKJobManager()
            st.sidebar.info("🧪 Running in mock mode")
        else:
            job_manager = SDKJobManager(profile=DATABRICKS_PROFILE)
        
        # Initialize database
        db = SimpleDatabase()
        db.initialize()
        
        return job_manager, db
        
    except Exception as e:
        st.error(f"❌ Failed to initialize app: {str(e)}")
        st.info("💡 Try setting USE_MOCK_JOBS=true for testing")
        st.stop()

def main():
    """Main application"""
    
    # Initialize
    job_manager, db = initialize_app()
    
    # Sidebar
    st.sidebar.title("🚀 Job Approval Workflow")
    page = st.sidebar.selectbox("Navigate:", ["Trigger Jobs", "Approvals", "History"])
    
    # User info
    if "user_id" not in st.session_state:
        st.session_state.user_id = st.sidebar.text_input("User ID:", value="user_001")
    
    if "user_role" not in st.session_state:
        st.session_state.user_role = st.sidebar.selectbox("Role:", ["requester", "approver", "admin"])
    
    # Connection status
    if job_manager.validate_connection():
        st.sidebar.success("✅ Connected to Databricks")
    else:
        st.sidebar.error("❌ Connection failed")
    
    # Route to pages
    if page == "Trigger Jobs":
        trigger_jobs_page(job_manager, db)
    elif page == "Approvals":
        approvals_page(job_manager, db)
    elif page == "History":
        history_page(db)

def trigger_jobs_page(job_manager, db):
    """Job triggering page"""
    st.header("🎯 Trigger Jobs with Approval")
    
    # Load jobs
    with st.spinner("Loading jobs..."):
        jobs = job_manager.list_jobs(limit=100)
    
    if not jobs:
        st.error("❌ No jobs found")
        return
    
    # Create job options
    job_options = {f"{job['name']} (ID: {job['job_id']})": job['job_id'] for job in jobs}
    
    # Find predefined jobs
    PREDEFINED_JOB1 = "72987522543057"
    PREDEFINED_JOB2 = "883505719297722"
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📋 Workflow Configuration")
        
        # Job 1 selection
        st.write("**First Job (Requires Approval)**")
        job1_options = list(job_options.keys())
        job1_default = next((i for i, opt in enumerate(job1_options) if PREDEFINED_JOB1 in opt), 0)
        
        job1_selection = st.selectbox("Select First Job:", job1_options, index=job1_default, key="job1")
        job1_id = job_options[job1_selection]
        job1_name = job1_selection.split(' (ID:')[0]
        
        job1_params = st.text_area("Job 1 Parameters (JSON):", 
                                  value='{"notebook_params": {"input_date": "2024-09-10"}}', 
                                  height=80, key="job1_params")
        
        st.divider()
        
        # Job 2 selection
        st.write("**Second Job (Auto-triggered After Approval)**")
        job2_default = next((i for i, opt in enumerate(job1_options) if PREDEFINED_JOB2 in opt), 1)
        
        job2_selection = st.selectbox("Select Second Job:", job1_options, index=job2_default, key="job2")
        job2_id = job_options[job2_selection]
        job2_name = job2_selection.split(' (ID:')[0]
        
        job2_params = st.text_area("Job 2 Parameters (JSON):", 
                                  value='{"notebook_params": {"model_name": "approval_model"}}', 
                                  height=80, key="job2_params")
    
    with col2:
        st.subheader("🚀 Execute Workflow")
        
        st.info(f"""
        **Workflow:**
        
        1️⃣ **{job1_name}**
        - Runs first, requires approval
        
        2️⃣ **{job2_name}**
        - Runs after approval
        
        👤 **User:** {st.session_state.user_id}
        """)
        
        # Validation
        valid = job1_id != job2_id and job1_id and job2_id
        
        if not valid:
            st.error("⚠️ Jobs must be different")
        
        if st.button("🎯 Start Workflow", type="primary", disabled=not valid):
            try:
                # Validate parameters
                params1 = json.loads(job1_params) if job1_params.strip() else {}
                params2 = json.loads(job2_params) if job2_params.strip() else {}
                
                # Create request
                request_id = str(uuid.uuid4())
                
                request_data = {
                    'request_id': request_id,
                    'requester_id': st.session_state.user_id,
                    'job1_id': job1_id,
                    'job1_name': job1_name,
                    'job1_params': json.dumps(params1),
                    'job2_id': job2_id,
                    'job2_name': job2_name,
                    'job2_params': json.dumps(params2),
                    'status': 'pending',
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat(),
                    'job1_status': '',
                    'job2_status': '',
                    'approver_id': '',
                    'rejection_reason': '',
                    'approval_comments': '',
                    'job1_run_id': '',
                    'job2_run_id': '',
                    'approved_at': '',
                    'rejected_at': ''
                }
                
                if db.create_request(request_data):
                    # Trigger first job
                    with st.spinner("Triggering first job..."):
                        job1_run_id = job_manager.trigger_job(job1_id, params1)
                    
                    if job1_run_id:
                        db.update_request(request_id, {
                            'job1_run_id': job1_run_id,
                            'job1_status': 'running'
                        })
                        
                        st.success(f"✅ Job 1 triggered! Run ID: `{job1_run_id}`")
                        st.info(f"📝 Request ID: `{request_id}`")
                        st.balloons()
                        
                        st.info("""
                        **Next Steps:**
                        1. 🔄 Job 1 is running
                        2. 👀 Check 'Approvals' page when Job 1 completes
                        3. ✅ Approver can then approve to trigger Job 2
                        """)
                        
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.error("❌ Failed to trigger Job 1")
                else:
                    st.error("❌ Failed to create request")
                    
            except json.JSONDecodeError:
                st.error("❌ Invalid JSON in parameters")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # Recent requests
    st.subheader("📋 Your Recent Requests")
    recent = db.get_user_requests(st.session_state.user_id, limit=5)
    
    if not recent.empty:
        display_cols = ['request_id', 'job1_name', 'job2_name', 'status', 'created_at']
        available_cols = [col for col in display_cols if col in recent.columns]
        display_df = recent[available_cols].copy()
        
        if 'request_id' in display_df.columns:
            display_df['request_id'] = display_df['request_id'].str[:8] + '...'
        if 'created_at' in display_df.columns:
            display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No recent requests")

def approvals_page(job_manager, db):
    """Approval dashboard"""
    st.header("✅ Approval Dashboard")
    
    if st.session_state.user_role not in ["approver", "admin"]:
        st.warning("⚠️ Need approver or admin role")
        return
    
    # Get pending approvals
    pending = db.get_pending_requests()
    
    if pending.empty:
        st.info("🎉 No pending approvals!")
        return
    
    st.subheader(f"📝 Pending Approvals ({len(pending)})")
    
    for _, row in pending.iterrows():
        with st.expander(f"📋 {row.get('job1_name', 'Unknown Job')} - {row['requester_id']}"):
            col1, col2, col3 = st.columns([1, 1, 1])
            
            with col1:
                st.write("**Request Info:**")
                st.write(f"**ID:** `{row['request_id'][:8]}...`")
                st.write(f"**User:** {row['requester_id']}")
                st.write(f"**Created:** {row['created_at']}")
                
                # Age calculation
                try:
                    created = pd.to_datetime(row['created_at'])
                    age_hours = (pd.Timestamp.now() - created).total_seconds() / 3600
                    st.write(f"**Age:** {age_hours:.1f} hours")
                except:
                    pass
            
            with col2:
                st.write("**Jobs:**")
                st.write(f"**Job 1:** {row.get('job1_name', 'Unknown')} (`{row['job1_id']}`)")
                st.write(f"**Job 2:** {row.get('job2_name', 'Unknown')} (`{row['job2_id']}`)")
                
                # Check job status
                job1_run_id = row.get('job1_run_id')
                if job1_run_id:
                    job1_status = job_manager.get_run_status(job1_run_id)
                    color = "green" if job1_status == "completed" else "orange" if job1_status == "running" else "red"
                    st.write(f"**Job 1 Status:** :{color}[{job1_status}]")
                else:
                    st.write("**Job 1 Status:** :gray[Not started]")
            
            with col3:
                st.write("**Actions:**")
                
                # Check if Job 1 is completed
                job1_completed = False
                if job1_run_id:
                    current_status = job_manager.get_run_status(job1_run_id)
                    job1_completed = current_status == 'completed'
                
                if job1_completed:
                    st.success("✅ Job 1 completed!")
                    
                    with st.form(key=f"approval_{row['request_id']}"):
                        comments = st.text_area("Comments:", placeholder="Optional approval comments...")
                        
                        col_a, col_r = st.columns(2)
                        
                        with col_a:
                            approved = st.form_submit_button("✅ Approve", type="primary")
                        with col_r:
                            rejected = st.form_submit_button("❌ Reject")
                        
                        if approved:
                            # Approve and trigger Job 2
                            db.update_request(row['request_id'], {
                                'status': 'approved',
                                'approver_id': st.session_state.user_id,
                                'approved_at': datetime.now().isoformat(),
                                'approval_comments': comments or '',
                                'updated_at': datetime.now().isoformat()
                            })
                            
                            # Trigger Job 2
                            with st.spinner("Triggering Job 2..."):
                                try:
                                    params2 = json.loads(row['job2_params'])
                                    job2_run_id = job_manager.trigger_job(row['job2_id'], params2)
                                    
                                    if job2_run_id:
                                        db.update_request(row['request_id'], {
                                            'job2_run_id': job2_run_id,
                                            'job2_status': 'running'
                                        })
                                        
                                        db.log_action(
                                            request_id=row['request_id'],
                                            job_id=row['job2_id'],
                                            action="job_triggered",
                                            user_id=st.session_state.user_id,
                                            job_type="job2",
                                            run_id=job2_run_id,
                                            details=f"Job 2 triggered after approval"
                                        )
                                        
                                        st.success(f"✅ Approved! Job 2 triggered: `{job2_run_id}`")
                                    else:
                                        st.error("❌ Job 2 trigger failed")
                                        
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
                            
                            time.sleep(1)
                            st.rerun()
                        
                        if rejected:
                            if not comments.strip():
                                st.error("⚠️ Please provide rejection reason")
                            else:
                                db.update_request(row['request_id'], {
                                    'status': 'rejected',
                                    'approver_id': st.session_state.user_id,
                                    'rejected_at': datetime.now().isoformat(),
                                    'rejection_reason': comments,
                                    'updated_at': datetime.now().isoformat()
                                })
                                
                                st.error("❌ Request rejected")
                                time.sleep(1)
                                st.rerun()
                
                else:
                    if job1_run_id:
                        status = job_manager.get_run_status(job1_run_id) or "unknown"
                        if status == "running":
                            st.info("⏳ Job 1 running...")
                            if st.button(f"🔄 Check Status", key=f"refresh_{row['request_id']}"):
                                st.rerun()
                        else:
                            st.info(f"ℹ️ Job 1 status: {status}")
                    else:
                        st.info("⏳ Job 1 not started")

def history_page(db):
    """History and statistics page"""
    st.header("📊 History & Stats")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        status_filter = st.selectbox("Status:", ["All", "pending", "approved", "rejected", "completed"])
    with col2:
        days_filter = st.selectbox("Days:", [7, 30, 90])
    with col3:
        if st.button("🔄 Refresh"):
            st.rerun()
    
    # Get history
    history = db.get_history(
        days=days_filter,
        status=None if status_filter == "All" else status_filter
    )
    
    if not history.empty:
        # Display table
        display_cols = ['request_id', 'requester_id', 'job1_name', 'job2_name', 'status', 'created_at', 'approver_id']
        available_cols = [col for col in display_cols if col in history.columns]
        display_df = history[available_cols].copy()
        
        if 'request_id' in display_df.columns:
            display_df['request_id'] = display_df['request_id'].str[:8] + '...'
        if 'created_at' in display_df.columns:
            display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        st.dataframe(display_df, use_container_width=True)
        
        # Statistics
        st.subheader("📈 Statistics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total", len(history))
        with col2:
            approved = len(history[history['status'] == 'approved'])
            st.metric("Approved", approved)
        with col3:
            rejected = len(history[history['status'] == 'rejected'])
            st.metric("Rejected", rejected)
        with col4:
            pending = len(history[history['status'] == 'pending'])
            st.metric("Pending", pending)
    
    else:
        st.info("No history found")
    
    # Database stats
    if st.session_state.user_role == "admin":
        st.subheader("🗄️ Database Stats")
        stats = db.get_stats()
        st.json(stats)
        
        if st.button("🗑️ Clear All Data", type="secondary"):
            if st.checkbox("Confirm deletion"):
                db.clear_all()
                st.success("✅ Data cleared")
                st.rerun()

if __name__ == "__main__":
    main()
