"""
Database Manager for Approval Workflow
Manages Delta tables in Databricks Lakehouse for storing approval states
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import os

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from delta.tables import DeltaTable
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️ PySpark not available. Running in local mode.")

class DatabaseManager:
    """Manages database operations for approval workflow"""
    
    def __init__(self, catalog: str = "main", schema: str = "approval_workflow"):
        self.catalog = catalog
        self.schema = schema
        self.table_prefix = f"{catalog}.{schema}"
        
        # Use instance variable to track Spark availability for this instance
        self.spark_available = SPARK_AVAILABLE
        
        if self.spark_available:
            try:
                self.spark = SparkSession.builder \
                    .appName("ApprovalWorkflowDB") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
                print("✅ PySpark session created successfully")
            except Exception as e:
                print(f"⚠️ Failed to create Spark session: {str(e)}")
                print("🔄 Falling back to local storage mode")
                self.spark_available = False
                self.spark = None
        
        if not self.spark_available or self.spark is None:
            # Local fallback using pandas and file storage
            self.data_dir = "local_data"
            os.makedirs(self.data_dir, exist_ok=True)
            self.spark = None
            print("💾 Using local CSV storage for approval workflow state")
    
    def initialize_tables(self):
        """Initialize Delta tables for approval workflow"""
        try:
            if self.spark_available and self.spark is not None:
                self._initialize_delta_tables()
            else:
                self._initialize_local_storage()
            print("✅ Database tables initialized successfully")
        except Exception as e:
            print(f"❌ Error initializing tables: {str(e)}")
            # Fallback to local storage if Delta tables fail
            if self.spark_available:
                print("🔄 Falling back to local storage due to error")
                self._initialize_local_storage()
    
    def _initialize_delta_tables(self):
        """Initialize Delta tables using Spark"""
        
        # Create schema if not exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        
        # Approval Requests table schema
        approval_requests_schema = StructType([
            StructField("request_id", StringType(), False),
            StructField("requester_id", StringType(), False),
            StructField("approver_id", StringType(), True),
            StructField("job1_id", StringType(), False),
            StructField("job1_params", StringType(), True),
            StructField("job1_run_id", StringType(), True),
            StructField("job1_status", StringType(), True),
            StructField("job2_id", StringType(), False),
            StructField("job2_params", StringType(), True),
            StructField("job2_run_id", StringType(), True),
            StructField("job2_status", StringType(), True),
            StructField("status", StringType(), False),  # pending, approved, rejected, completed
            StructField("rejection_reason", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("approved_at", TimestampType(), True),
            StructField("rejected_at", TimestampType(), True),
            StructField("completed_at", TimestampType(), True)
        ])
        
        # Create approval requests table
        approval_requests_table = f"{self.table_prefix}.approval_requests"
        if not self._table_exists(approval_requests_table):
            empty_df = self.spark.createDataFrame([], approval_requests_schema)
            empty_df.write.format("delta").mode("overwrite").saveAsTable(approval_requests_table)
        
        # Job Audit Log schema
        job_audit_schema = StructType([
            StructField("log_id", StringType(), False),
            StructField("request_id", StringType(), False),
            StructField("job_id", StringType(), False),
            StructField("run_id", StringType(), True),
            StructField("job_type", StringType(), False),  # job1 or job2
            StructField("action", StringType(), False),  # triggered, completed, failed
            StructField("status", StringType(), True),
            StructField("details", StringType(), True),
            StructField("user_id", StringType(), False),
            StructField("timestamp", TimestampType(), False)
        ])
        
        # Create job audit log table
        job_audit_table = f"{self.table_prefix}.job_audit_log"
        if not self._table_exists(job_audit_table):
            empty_df = self.spark.createDataFrame([], job_audit_schema)
            empty_df.write.format("delta").mode("overwrite").saveAsTable(job_audit_table)
    
    def _initialize_local_storage(self):
        """Initialize local CSV storage as fallback"""
        # Create empty CSV files with headers if they don't exist
        
        approval_requests_df = pd.DataFrame(columns=[
            'request_id', 'requester_id', 'approver_id', 'job1_id', 'job1_name', 'job1_params',
            'job1_run_id', 'job1_status', 'job2_id', 'job2_name', 'job2_params', 'job2_run_id',
            'job2_status', 'status', 'rejection_reason', 'approval_comments', 'created_at', 'updated_at',
            'approved_at', 'rejected_at', 'completed_at'
        ])
        
        job_audit_df = pd.DataFrame(columns=[
            'log_id', 'request_id', 'job_id', 'run_id', 'job_type', 'action',
            'status', 'details', 'user_id', 'timestamp'
        ])
        
        approval_requests_file = f"{self.data_dir}/approval_requests.csv"
        job_audit_file = f"{self.data_dir}/job_audit_log.csv"
        
        if not os.path.exists(approval_requests_file):
            approval_requests_df.to_csv(approval_requests_file, index=False)
        
        if not os.path.exists(job_audit_file):
            job_audit_df.to_csv(job_audit_file, index=False)
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            self.spark.table(table_name).limit(1).collect()
            return True
        except:
            return False
    
    def create_approval_request(self, approval_data: Dict[str, Any]) -> bool:
        """Create a new approval request"""
        try:
            if self.spark_available and self.spark is not None:
                return self._create_approval_request_delta(approval_data)
            else:
                return self._create_approval_request_local(approval_data)
        except Exception as e:
            print(f"❌ Error creating approval request: {str(e)}")
            return False
    
    def _create_approval_request_delta(self, approval_data: Dict[str, Any]) -> bool:
        """Create approval request in Delta table"""
        # Convert datetime strings to timestamps
        approval_data['created_at'] = pd.to_datetime(approval_data['created_at'])
        approval_data['updated_at'] = pd.to_datetime(approval_data['updated_at'])
        
        df = self.spark.createDataFrame([approval_data])
        df.write.format("delta").mode("append").saveAsTable(f"{self.table_prefix}.approval_requests")
        
        # Log the action
        self.log_job_action(
            request_id=approval_data['request_id'],
            job_id=approval_data['job1_id'],
            job_type="job1",
            action="request_created",
            user_id=approval_data['requester_id'],
            details=f"Approval request created for jobs {approval_data['job1_id']} -> {approval_data['job2_id']}"
        )
        
        return True
    
    def _create_approval_request_local(self, approval_data: Dict[str, Any]) -> bool:
        """Create approval request in local CSV"""
        file_path = f"{self.data_dir}/approval_requests.csv"
        
        # Read existing data
        try:
            df = pd.read_csv(file_path)
        except:
            df = pd.DataFrame()
        
        # Add new request
        new_df = pd.DataFrame([approval_data])
        df = pd.concat([df, new_df], ignore_index=True)
        
        # Save back to CSV
        df.to_csv(file_path, index=False)
        
        # Log the action
        self.log_job_action(
            request_id=approval_data['request_id'],
            job_id=approval_data['job1_id'],
            job_type="job1",
            action="request_created",
            user_id=approval_data['requester_id'],
            details=f"Approval request created for jobs {approval_data['job1_id']} -> {approval_data['job2_id']}"
        )
        
        return True
    
    def update_approval_request(self, request_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing approval request"""
        try:
            if self.spark_available and self.spark is not None:
                return self._update_approval_request_delta(request_id, updates)
            else:
                return self._update_approval_request_local(request_id, updates)
        except Exception as e:
            print(f"❌ Error updating approval request: {str(e)}")
            return False
    
    def _update_approval_request_delta(self, request_id: str, updates: Dict[str, Any]) -> bool:
        """Update approval request in Delta table"""
        table_name = f"{self.table_prefix}.approval_requests"
        
        # Build update expression
        update_expr = {}
        for key, value in updates.items():
            if isinstance(value, str) and key.endswith('_at'):
                update_expr[key] = pd.to_datetime(value)
            else:
                update_expr[key] = value
        
        # Perform update using Delta table
        delta_table = DeltaTable.forName(self.spark, table_name)
        delta_table.update(
            condition=f"request_id = '{request_id}'",
            set=update_expr
        )
        
        return True
    
    def _update_approval_request_local(self, request_id: str, updates: Dict[str, Any]) -> bool:
        """Update approval request in local CSV"""
        file_path = f"{self.data_dir}/approval_requests.csv"
        
        # Read existing data
        df = pd.read_csv(file_path)
        
        # Find and update the row
        mask = df['request_id'] == request_id
        for key, value in updates.items():
            df.loc[mask, key] = value
        
        # Save back to CSV
        df.to_csv(file_path, index=False)
        
        return True
    
    def get_pending_approvals(self) -> pd.DataFrame:
        """Get all pending approval requests"""
        try:
            if self.spark_available and self.spark is not None:
                df = self.spark.table(f"{self.table_prefix}.approval_requests") \
                    .filter("status = 'pending'") \
                    .orderBy(desc("created_at")) \
                    .toPandas()
            else:
                file_path = f"{self.data_dir}/approval_requests.csv"
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    df = df[df['status'] == 'pending'].sort_values('created_at', ascending=False)
                else:
                    df = pd.DataFrame()
            
            return df
        except Exception as e:
            print(f"❌ Error getting pending approvals: {str(e)}")
            return pd.DataFrame()
    
    def get_user_requests(self, user_id: str, limit: int = 10) -> pd.DataFrame:
        """Get recent requests for a specific user"""
        try:
            if self.spark_available and self.spark is not None:
                df = self.spark.table(f"{self.table_prefix}.approval_requests") \
                    .filter(f"requester_id = '{user_id}'") \
                    .orderBy(desc("created_at")) \
                    .limit(limit) \
                    .toPandas()
            else:
                file_path = f"{self.data_dir}/approval_requests.csv"
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    df = df[df['requester_id'] == user_id] \
                        .sort_values('created_at', ascending=False) \
                        .head(limit)
                else:
                    df = pd.DataFrame()
            
            return df
        except Exception as e:
            print(f"❌ Error getting user requests: {str(e)}")
            return pd.DataFrame()
    
    def get_request_history(self, status: Optional[str] = None, 
                          user_id: Optional[str] = None, days: int = 30) -> pd.DataFrame:
        """Get request history with optional filters"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            if self.spark_available and self.spark is not None:
                query = self.spark.table(f"{self.table_prefix}.approval_requests") \
                    .filter(f"created_at >= '{cutoff_date.isoformat()}'")
                
                if status:
                    query = query.filter(f"status = '{status}'")
                
                if user_id:
                    query = query.filter(f"requester_id = '{user_id}'")
                
                df = query.orderBy(desc("created_at")).toPandas()
            else:
                file_path = f"{self.data_dir}/approval_requests.csv"
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    df['created_at'] = pd.to_datetime(df['created_at'])
                    
                    # Apply filters
                    df = df[df['created_at'] >= cutoff_date]
                    
                    if status:
                        df = df[df['status'] == status]
                    
                    if user_id:
                        df = df[df['requester_id'] == user_id]
                    
                    df = df.sort_values('created_at', ascending=False)
                else:
                    df = pd.DataFrame()
            
            return df
        except Exception as e:
            print(f"❌ Error getting request history: {str(e)}")
            return pd.DataFrame()
    
    def log_job_action(self, request_id: str, job_id: str, job_type: str, 
                      action: str, user_id: str, details: str = "", 
                      run_id: Optional[str] = None, status: Optional[str] = None):
        """Log job action to audit table"""
        try:
            import uuid
            
            log_data = {
                'log_id': str(uuid.uuid4()),
                'request_id': request_id,
                'job_id': job_id,
                'run_id': run_id,
                'job_type': job_type,
                'action': action,
                'status': status,
                'details': details,
                'user_id': user_id,
                'timestamp': datetime.now().isoformat()
            }
            
            if self.spark_available and self.spark is not None:
                log_data['timestamp'] = pd.to_datetime(log_data['timestamp'])
                df = self.spark.createDataFrame([log_data])
                df.write.format("delta").mode("append").saveAsTable(f"{self.table_prefix}.job_audit_log")
            else:
                file_path = f"{self.data_dir}/job_audit_log.csv"
                
                try:
                    df = pd.read_csv(file_path)
                except:
                    df = pd.DataFrame()
                
                new_df = pd.DataFrame([log_data])
                df = pd.concat([df, new_df], ignore_index=True)
                df.to_csv(file_path, index=False)
                
        except Exception as e:
            print(f"⚠️ Warning: Failed to log job action: {str(e)}")
    
    def get_table_stats(self) -> Dict[str, Any]:
        """Get statistics about the tables"""
        try:
            stats = {}
            
            if self.spark_available and self.spark is not None:
                # Approval requests stats
                approval_count = self.spark.table(f"{self.table_prefix}.approval_requests").count()
                pending_count = self.spark.table(f"{self.table_prefix}.approval_requests") \
                    .filter("status = 'pending'").count()
                
                # Audit log stats
                audit_count = self.spark.table(f"{self.table_prefix}.job_audit_log").count()
                
                stats = {
                    "approval_requests_total": approval_count,
                    "approval_requests_pending": pending_count,
                    "audit_log_entries": audit_count,
                    "storage_mode": "delta_tables",
                    "last_updated": datetime.now().isoformat()
                }
            else:
                # Local file stats
                approval_file = f"{self.data_dir}/approval_requests.csv"
                audit_file = f"{self.data_dir}/job_audit_log.csv"
                
                approval_df = pd.read_csv(approval_file) if os.path.exists(approval_file) else pd.DataFrame()
                audit_df = pd.read_csv(audit_file) if os.path.exists(audit_file) else pd.DataFrame()
                
                stats = {
                    "approval_requests_total": len(approval_df),
                    "approval_requests_pending": len(approval_df[approval_df.get('status') == 'pending']) if not approval_df.empty else 0,
                    "audit_log_entries": len(audit_df),
                    "storage_mode": "local_csv",
                    "last_updated": datetime.now().isoformat()
                }
            
            return stats
        except Exception as e:
            return {"error": str(e)}
    
    def clear_all_data(self):
        """Clear all data (admin function)"""
        try:
            if self.spark_available and self.spark is not None:
                self.spark.sql(f"DELETE FROM {self.table_prefix}.approval_requests")
                self.spark.sql(f"DELETE FROM {self.table_prefix}.job_audit_log")
            else:
                # Clear local files
                approval_file = f"{self.data_dir}/approval_requests.csv"
                audit_file = f"{self.data_dir}/job_audit_log.csv"
                
                pd.DataFrame(columns=[
                    'request_id', 'requester_id', 'approver_id', 'job1_id', 'job1_name', 'job1_params',
                    'job1_run_id', 'job1_status', 'job2_id', 'job2_name', 'job2_params', 'job2_run_id',
                    'job2_status', 'status', 'rejection_reason', 'approval_comments', 'created_at', 'updated_at',
                    'approved_at', 'rejected_at', 'completed_at'
                ]).to_csv(approval_file, index=False)
                
                pd.DataFrame(columns=[
                    'log_id', 'request_id', 'job_id', 'run_id', 'job_type', 'action',
                    'status', 'details', 'user_id', 'timestamp'
                ]).to_csv(audit_file, index=False)
            
            print("✅ All data cleared successfully")
        except Exception as e:
            print(f"❌ Error clearing data: {str(e)}")
