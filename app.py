"""
Databricks App for Job Approval Workflow
Main application entry point
"""

import streamlit as st
import os
from datetime import datetime
import pandas as pd
import requests
import json
from typing import Dict, List, Optional
import uuid

# Databricks API configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Page configuration
st.set_page_config(
    page_title="Databricks Job Approval Workflow",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import custom modules
from backend.job_manager import JobManager
from backend.approval_manager import ApprovalManager
from backend.database_manager import DatabaseManager

def main():
    """Main application function"""
    
    # Initialize managers
    job_manager = JobManager(DATABRICKS_HOST, DATABRICKS_TOKEN)
    approval_manager = ApprovalManager()
    db_manager = DatabaseManager()
    
    # Initialize database tables
    db_manager.initialize_tables()
    
    # Sidebar navigation
    st.sidebar.title("🚀 Job Approval Workflow")
    page = st.sidebar.selectbox(
        "Navigate to:",
        ["Job Triggers", "Approval Dashboard", "Job History", "Admin Panel"]
    )
    
    # User authentication (simplified)
    if "user_id" not in st.session_state:
        st.session_state.user_id = st.sidebar.text_input("Enter User ID:", value="user_001")
    
    if "user_role" not in st.session_state:
        st.session_state.user_role = st.sidebar.selectbox("Role:", ["requester", "approver", "admin"])
    
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
    """Render the job triggers page"""
    st.header("🎯 Trigger Databricks Jobs")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📋 Job Configuration")
        
        # Job 1 Configuration
        st.write("**First Job (Requires Approval)**")
        job1_id = st.text_input("Job 1 ID:", value="12345", key="job1_id")
        job1_params = st.text_area("Job 1 Parameters (JSON):", 
                                  value='{"param1": "value1"}', key="job1_params")
        
        # Job 2 Configuration  
        st.write("**Second Job (Auto-triggered after approval)**")
        job2_id = st.text_input("Job 2 ID:", value="67890", key="job2_id")
        job2_params = st.text_area("Job 2 Parameters (JSON):", 
                                  value='{"param2": "value2"}', key="job2_params")
    
    with col2:
        st.subheader("🚀 Execution")
        
        if st.button("🎯 Trigger First Job", type="primary"):
            if job1_id and job2_id:
                try:
                    # Create approval request
                    request_id = str(uuid.uuid4())
                    
                    # Store approval request in database
                    approval_data = {
                        'request_id': request_id,
                        'requester_id': st.session_state.user_id,
                        'job1_id': job1_id,
                        'job1_params': job1_params,
                        'job2_id': job2_id,
                        'job2_params': job2_params,
                        'status': 'pending',
                        'created_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat()
                    }
                    
                    db_manager.create_approval_request(approval_data)
                    
                    # Trigger first job
                    job1_run_id = job_manager.trigger_job(job1_id, json.loads(job1_params))
                    
                    if job1_run_id:
                        # Update approval request with job1 run ID
                        db_manager.update_approval_request(request_id, {
                            'job1_run_id': job1_run_id,
                            'job1_status': 'running'
                        })
                        
                        st.success(f"✅ Job 1 triggered successfully! Run ID: {job1_run_id}")
                        st.success(f"📝 Approval request created: {request_id}")
                        st.info("🔄 Job 1 is running. Once completed, it will require approval before Job 2 can be triggered.")
                    else:
                        st.error("❌ Failed to trigger Job 1")
                        
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
            else:
                st.error("⚠️ Please provide both Job IDs")
    
    # Show recent requests for current user
    st.subheader("📋 Your Recent Requests")
    recent_requests = db_manager.get_user_requests(st.session_state.user_id, limit=5)
    if recent_requests:
        st.dataframe(recent_requests)
    else:
        st.info("No recent requests found.")

def render_approval_dashboard(approval_manager, db_manager, job_manager):
    """Render the approval dashboard"""
    st.header("✅ Approval Dashboard")
    
    if st.session_state.user_role not in ["approver", "admin"]:
        st.warning("⚠️ You need approver or admin role to access this page.")
        return
    
    # Get pending approvals
    pending_approvals = db_manager.get_pending_approvals()
    
    if pending_approvals.empty:
        st.info("🎉 No pending approvals at this time!")
        return
    
    st.subheader(f"📝 Pending Approvals ({len(pending_approvals)})")
    
    for idx, row in pending_approvals.iterrows():
        with st.expander(f"📋 Request {row['request_id'][:8]}... - {row['requester_id']}"):
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                st.write(f"**Requester:** {row['requester_id']}")
                st.write(f"**Created:** {row['created_at']}")
                st.write(f"**Job 1 ID:** {row['job1_id']}")
                st.write(f"**Job 1 Status:** {row.get('job1_status', 'Unknown')}")
                
                # Check Job 1 status
                if row.get('job1_run_id'):
                    job1_status = job_manager.get_job_run_status(row['job1_run_id'])
                    st.write(f"**Job 1 Run Status:** {job1_status}")
            
            with col2:
                st.write(f"**Job 2 ID:** {row['job2_id']}")
                st.write("**Job 1 Parameters:**")
                st.code(row['job1_params'])
                st.write("**Job 2 Parameters:**")
                st.code(row['job2_params'])
            
            with col3:
                # Only show approval buttons if Job 1 is completed successfully
                job1_completed = row.get('job1_status') == 'completed'
                
                if st.button("✅ Approve", key=f"approve_{row['request_id']}", 
                            disabled=not job1_completed):
                    # Update approval status
                    db_manager.update_approval_request(row['request_id'], {
                        'status': 'approved',
                        'approver_id': st.session_state.user_id,
                        'approved_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat()
                    })
                    
                    # Trigger Job 2
                    try:
                        job2_params = json.loads(row['job2_params'])
                        job2_run_id = job_manager.trigger_job(row['job2_id'], job2_params)
                        
                        if job2_run_id:
                            db_manager.update_approval_request(row['request_id'], {
                                'job2_run_id': job2_run_id,
                                'job2_status': 'running'
                            })
                            st.success(f"✅ Approved! Job 2 triggered with Run ID: {job2_run_id}")
                        else:
                            st.error("❌ Approval recorded but failed to trigger Job 2")
                    except Exception as e:
                        st.error(f"❌ Error triggering Job 2: {str(e)}")
                    
                    st.rerun()
                
                if st.button("❌ Reject", key=f"reject_{row['request_id']}"):
                    reason = st.text_input(f"Rejection reason for {row['request_id'][:8]}:", 
                                         key=f"reason_{row['request_id']}")
                    if reason:
                        db_manager.update_approval_request(row['request_id'], {
                            'status': 'rejected',
                            'approver_id': st.session_state.user_id,
                            'rejection_reason': reason,
                            'rejected_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat()
                        })
                        st.error("❌ Request rejected")
                        st.rerun()
                
                if not job1_completed:
                    st.info("⏳ Waiting for Job 1 to complete")

def render_job_history(db_manager):
    """Render job history page"""
    st.header("📊 Job History")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        status_filter = st.selectbox("Status:", ["All", "pending", "approved", "rejected", "completed"])
    with col2:
        user_filter = st.text_input("User ID Filter:")
    with col3:
        days_filter = st.selectbox("Last N days:", [7, 30, 90, 365])
    
    # Get filtered history
    history = db_manager.get_request_history(
        status=None if status_filter == "All" else status_filter,
        user_id=user_filter if user_filter else None,
        days=days_filter
    )
    
    if not history.empty:
        st.dataframe(history, use_container_width=True)
        
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
    else:
        st.info("No history found for the selected filters.")

def render_admin_panel(db_manager, job_manager):
    """Render admin panel"""
    st.header("⚙️ Admin Panel")
    
    if st.session_state.user_role != "admin":
        st.warning("⚠️ Admin access required.")
        return
    
    tab1, tab2, tab3 = st.tabs(["Database Management", "Job Configuration", "System Health"])
    
    with tab1:
        st.subheader("🗄️ Database Management")
        
        if st.button("🔄 Refresh Tables"):
            db_manager.initialize_tables()
            st.success("✅ Tables refreshed")
        
        if st.button("🗑️ Clear All Data"):
            if st.checkbox("I understand this will delete all data"):
                db_manager.clear_all_data()
                st.success("✅ All data cleared")
        
        # Show table stats
        stats = db_manager.get_table_stats()
        st.json(stats)
    
    with tab2:
        st.subheader("⚙️ Job Configuration")
        
        # Test job connectivity
        if st.button("🔍 Test Job API Connection"):
            try:
                jobs = job_manager.list_jobs()
                if jobs:
                    st.success(f"✅ Connected! Found {len(jobs)} jobs")
                    st.json(jobs[:3])  # Show first 3 jobs
                else:
                    st.warning("⚠️ Connected but no jobs found")
            except Exception as e:
                st.error(f"❌ Connection failed: {str(e)}")
    
    with tab3:
        st.subheader("🏥 System Health")
        
        # System metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Active Sessions", 1)  # Simplified
            st.metric("Database Status", "Healthy")
        
        with col2:
            st.metric("API Status", "Connected")
            st.metric("Last Updated", datetime.now().strftime("%H:%M:%S"))

if __name__ == "__main__":
    main()
