"""
Simplified Database Manager - CSV Only
Pure pandas-based storage without Spark dependencies
"""

import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import uuid


class SimpleDatabase:
    """Simplified database manager using only CSV storage"""
    
    def __init__(self, data_dir: str = "local_data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.approval_file = os.path.join(self.data_dir, "approvals.csv")
        self.audit_file = os.path.join(self.data_dir, "audit.csv")
        print(f"💾 Using simplified CSV storage in: {self.data_dir}")
    
    def initialize(self):
        """Initialize CSV files with proper headers"""
        try:
            # Approval requests table
            if not os.path.exists(self.approval_file):
                approval_df = pd.DataFrame(columns=[
                    'request_id', 'requester_id', 'approver_id', 
                    'job1_id', 'job1_name', 'job1_params', 'job1_run_id', 'job1_status',
                    'job2_id', 'job2_name', 'job2_params', 'job2_run_id', 'job2_status',
                    'status', 'rejection_reason', 'approval_comments',
                    'created_at', 'updated_at', 'approved_at', 'rejected_at'
                ])
                approval_df.to_csv(self.approval_file, index=False)
            
            # Audit log table
            if not os.path.exists(self.audit_file):
                audit_df = pd.DataFrame(columns=[
                    'log_id', 'request_id', 'job_id', 'run_id', 'job_type', 
                    'action', 'status', 'details', 'user_id', 'timestamp'
                ])
                audit_df.to_csv(self.audit_file, index=False)
            
            print("✅ Database initialized successfully")
        except Exception as e:
            print(f"❌ Error initializing database: {str(e)}")
    
    def create_request(self, data: Dict[str, Any]) -> bool:
        """Create a new approval request"""
        try:
            df = pd.read_csv(self.approval_file) if os.path.exists(self.approval_file) else pd.DataFrame()
            new_df = pd.DataFrame([data])
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(self.approval_file, index=False)
            
            self.log_action(
                request_id=data['request_id'],
                job_id=data['job1_id'],
                action="request_created",
                user_id=data['requester_id'],
                details=f"Request created for {data.get('job1_name', 'Unknown Job')}"
            )
            return True
        except Exception as e:
            print(f"❌ Error creating request: {str(e)}")
            return False
    
    def update_request(self, request_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing request"""
        try:
            df = pd.read_csv(self.approval_file)
            mask = df['request_id'] == request_id
            
            for key, value in updates.items():
                df.loc[mask, key] = value
            
            df.to_csv(self.approval_file, index=False)
            return True
        except Exception as e:
            print(f"❌ Error updating request: {str(e)}")
            return False
    
    def get_pending_requests(self) -> pd.DataFrame:
        """Get pending approval requests"""
        try:
            if os.path.exists(self.approval_file):
                df = pd.read_csv(self.approval_file)
                pending = df[df['status'] == 'pending'].copy()
                return pending.sort_values('created_at', ascending=False) if not pending.empty else pd.DataFrame()
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ Error getting pending requests: {str(e)}")
            return pd.DataFrame()
    
    def get_user_requests(self, user_id: str, limit: int = 10) -> pd.DataFrame:
        """Get requests for a specific user"""
        try:
            if os.path.exists(self.approval_file):
                df = pd.read_csv(self.approval_file)
                user_requests = df[df['requester_id'] == user_id].copy()
                return user_requests.sort_values('created_at', ascending=False).head(limit) if not user_requests.empty else pd.DataFrame()
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ Error getting user requests: {str(e)}")
            return pd.DataFrame()
    
    def get_history(self, days: int = 30, status: Optional[str] = None, user_id: Optional[str] = None) -> pd.DataFrame:
        """Get request history with filters"""
        try:
            if not os.path.exists(self.approval_file):
                return pd.DataFrame()
            
            df = pd.read_csv(self.approval_file)
            if df.empty:
                return df
            
            # Convert created_at to datetime
            df['created_at'] = pd.to_datetime(df['created_at'])
            
            # Filter by date
            cutoff_date = datetime.now() - timedelta(days=days)
            df = df[df['created_at'] >= cutoff_date]
            
            # Apply filters
            if status:
                df = df[df['status'] == status]
            if user_id:
                df = df[df['requester_id'] == user_id]
            
            return df.sort_values('created_at', ascending=False)
        except Exception as e:
            print(f"❌ Error getting history: {str(e)}")
            return pd.DataFrame()
    
    def log_action(self, request_id: str, job_id: str, action: str, user_id: str, 
                  details: str = "", job_type: str = "job1", run_id: Optional[str] = None, 
                  status: Optional[str] = None):
        """Log an action to audit trail"""
        try:
            log_data = {
                'log_id': str(uuid.uuid4()),
                'request_id': request_id,
                'job_id': job_id,
                'run_id': run_id or '',
                'job_type': job_type,
                'action': action,
                'status': status or '',
                'details': details,
                'user_id': user_id,
                'timestamp': datetime.now().isoformat()
            }
            
            df = pd.read_csv(self.audit_file) if os.path.exists(self.audit_file) else pd.DataFrame()
            new_df = pd.DataFrame([log_data])
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(self.audit_file, index=False)
        except Exception as e:
            print(f"⚠️ Warning: Failed to log action: {str(e)}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            stats = {"storage_mode": "simplified_csv", "last_updated": datetime.now().isoformat()}
            
            if os.path.exists(self.approval_file):
                df = pd.read_csv(self.approval_file)
                stats.update({
                    "total_requests": len(df),
                    "pending_requests": len(df[df['status'] == 'pending']) if not df.empty else 0,
                    "approved_requests": len(df[df['status'] == 'approved']) if not df.empty else 0,
                    "completed_requests": len(df[df['status'] == 'completed']) if not df.empty else 0
                })
            else:
                stats.update({
                    "total_requests": 0,
                    "pending_requests": 0, 
                    "approved_requests": 0,
                    "completed_requests": 0
                })
            
            if os.path.exists(self.audit_file):
                audit_df = pd.read_csv(self.audit_file)
                stats["audit_entries"] = len(audit_df)
            else:
                stats["audit_entries"] = 0
            
            return stats
        except Exception as e:
            return {"error": str(e)}
    
    def clear_all(self):
        """Clear all data"""
        try:
            if os.path.exists(self.approval_file):
                os.remove(self.approval_file)
            if os.path.exists(self.audit_file):
                os.remove(self.audit_file)
            self.initialize()
            print("✅ All data cleared")
        except Exception as e:
            print(f"❌ Error clearing data: {str(e)}")
