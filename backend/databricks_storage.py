"""
Databricks Native Storage Manager
Uses Unity Catalog tables for state management within Databricks Apps
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import uuid

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, desc, current_timestamp
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

class DatabricksStorage:
    """Native Databricks storage using Unity Catalog tables"""
    
    def __init__(self, catalog: str = "main", schema: str = "approval_workflow"):
        self.catalog = catalog
        self.schema = schema
        self.approval_table = f"{catalog}.{schema}.approval_requests"
        self.audit_table = f"{catalog}.{schema}.audit_log"
        
        if SPARK_AVAILABLE:
            self.spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ApprovalWorkflow").getOrCreate()
            print(f"✅ Using Unity Catalog: {catalog}.{schema}")
        else:
            raise RuntimeError("❌ Spark session required for Databricks Apps")
    
    def initialize(self):
        """Initialize Unity Catalog tables"""
        try:
            # Create schema if not exists
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
            
            # Create approval requests table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.approval_table} (
                    request_id STRING NOT NULL,
                    requester_id STRING NOT NULL,
                    approver_id STRING,
                    job1_id STRING NOT NULL,
                    job1_name STRING,
                    job1_params STRING,
                    job1_run_id STRING,
                    job1_status STRING,
                    job2_id STRING NOT NULL,
                    job2_name STRING,
                    job2_params STRING,
                    job2_run_id STRING,
                    job2_status STRING,
                    status STRING NOT NULL,
                    rejection_reason STRING,
                    approval_comments STRING,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    approved_at TIMESTAMP,
                    rejected_at TIMESTAMP
                ) USING DELTA
                TBLPROPERTIES (
                    'delta.feature.allowColumnDefaults' = 'supported'
                )
            """)
            
            # Create audit log table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.audit_table} (
                    log_id STRING NOT NULL,
                    request_id STRING NOT NULL,
                    job_id STRING,
                    run_id STRING,
                    job_type STRING,
                    action STRING NOT NULL,
                    status STRING,
                    details STRING,
                    user_id STRING NOT NULL,
                    timestamp TIMESTAMP NOT NULL
                ) USING DELTA
                TBLPROPERTIES (
                    'delta.feature.allowColumnDefaults' = 'supported'
                )
            """)
            
            print("✅ Unity Catalog tables initialized")
        except Exception as e:
            print(f"❌ Error initializing tables: {str(e)}")
            raise
    
    def create_request(self, data: Dict[str, Any]) -> bool:
        """Create approval request in Unity Catalog"""
        try:
            # Convert to Spark DataFrame
            df_data = [(
                data['request_id'],
                data['requester_id'],
                data.get('approver_id'),
                data['job1_id'],
                data.get('job1_name'),
                data.get('job1_params', '{}'),
                data.get('job1_run_id'),
                data.get('job1_status', 'pending'),
                data['job2_id'],
                data.get('job2_name'),
                data.get('job2_params', '{}'),
                data.get('job2_run_id'),
                data.get('job2_status', 'pending'),
                data.get('status', 'pending'),
                data.get('rejection_reason'),
                data.get('approval_comments'),
                datetime.fromisoformat(data['created_at'].replace('Z', '+00:00')),
                datetime.fromisoformat(data['updated_at'].replace('Z', '+00:00')),
                None,  # approved_at
                None   # rejected_at
            )]
            
            schema = StructType([
                StructField("request_id", StringType(), False),
                StructField("requester_id", StringType(), False),
                StructField("approver_id", StringType(), True),
                StructField("job1_id", StringType(), False),
                StructField("job1_name", StringType(), True),
                StructField("job1_params", StringType(), True),
                StructField("job1_run_id", StringType(), True),
                StructField("job1_status", StringType(), True),
                StructField("job2_id", StringType(), False),
                StructField("job2_name", StringType(), True),
                StructField("job2_params", StringType(), True),
                StructField("job2_run_id", StringType(), True),
                StructField("job2_status", StringType(), True),
                StructField("status", StringType(), False),
                StructField("rejection_reason", StringType(), True),
                StructField("approval_comments", StringType(), True),
                StructField("created_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), False),
                StructField("approved_at", TimestampType(), True),
                StructField("rejected_at", TimestampType(), True)
            ])
            
            df = self.spark.createDataFrame(df_data, schema)
            df.write.mode("append").saveAsTable(self.approval_table)
            
            # Log the action
            self.log_action(
                request_id=data['request_id'],
                job_id=data['job1_id'],
                action="request_created",
                user_id=data['requester_id'],
                details=f"Created request for {data.get('job1_name', 'Unknown Job')}"
            )
            
            return True
        except Exception as e:
            print(f"❌ Error creating request: {str(e)}")
            return False
    
    def update_request(self, request_id: str, updates: Dict[str, Any]) -> bool:
        """Update approval request"""
        try:
            # Build update SQL
            update_clauses = []
            for key, value in updates.items():
                if key.endswith('_at') and value:
                    # Handle timestamp fields
                    if isinstance(value, str):
                        update_clauses.append(f"{key} = TIMESTAMP '{value}'")
                    else:
                        update_clauses.append(f"{key} = CURRENT_TIMESTAMP()")
                elif value is not None:
                    update_clauses.append(f"{key} = '{value}'")
            
            if update_clauses:
                update_sql = f"""
                    UPDATE {self.approval_table}
                    SET {', '.join(update_clauses)}, updated_at = CURRENT_TIMESTAMP()
                    WHERE request_id = '{request_id}'
                """
                self.spark.sql(update_sql)
            
            return True
        except Exception as e:
            print(f"❌ Error updating request: {str(e)}")
            return False
    
    def get_pending_requests(self) -> pd.DataFrame:
        """Get pending approval requests"""
        try:
            df = self.spark.sql(f"""
                SELECT * FROM {self.approval_table}
                WHERE status = 'pending'
                ORDER BY created_at DESC
            """)
            return df.toPandas()
        except Exception as e:
            print(f"❌ Error getting pending requests: {str(e)}")
            return pd.DataFrame()
    
    def get_user_requests(self, user_id: str, limit: int = 10) -> pd.DataFrame:
        """Get user's requests"""
        try:
            df = self.spark.sql(f"""
                SELECT * FROM {self.approval_table}
                WHERE requester_id = '{user_id}'
                ORDER BY created_at DESC
                LIMIT {limit}
            """)
            return df.toPandas()
        except Exception as e:
            print(f"❌ Error getting user requests: {str(e)}")
            return pd.DataFrame()
    
    def get_history(self, days: int = 30, status: Optional[str] = None, user_id: Optional[str] = None) -> pd.DataFrame:
        """Get request history"""
        try:
            where_clauses = [f"created_at >= CURRENT_TIMESTAMP() - INTERVAL {days} DAYS"]
            
            if status:
                where_clauses.append(f"status = '{status}'")
            if user_id:
                where_clauses.append(f"requester_id = '{user_id}'")
            
            where_clause = " AND ".join(where_clauses)
            
            df = self.spark.sql(f"""
                SELECT * FROM {self.approval_table}
                WHERE {where_clause}
                ORDER BY created_at DESC
            """)
            return df.toPandas()
        except Exception as e:
            print(f"❌ Error getting history: {str(e)}")
            return pd.DataFrame()
    
    def log_action(self, request_id: str, job_id: str, action: str, user_id: str, 
                  details: str = "", job_type: str = "job1", run_id: Optional[str] = None, 
                  status: Optional[str] = None):
        """Log action to audit table"""
        try:
            log_data = [(
                str(uuid.uuid4()),
                request_id,
                job_id or "",
                run_id or "",
                job_type,
                action,
                status or "",
                details,
                user_id,
                datetime.now()
            )]
            
            schema = StructType([
                StructField("log_id", StringType(), False),
                StructField("request_id", StringType(), False),
                StructField("job_id", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("job_type", StringType(), True),
                StructField("action", StringType(), False),
                StructField("status", StringType(), True),
                StructField("details", StringType(), True),
                StructField("user_id", StringType(), False),
                StructField("timestamp", TimestampType(), False)
            ])
            
            df = self.spark.createDataFrame(log_data, schema)
            df.write.mode("append").saveAsTable(self.audit_table)
        except Exception as e:
            print(f"⚠️ Failed to log action: {str(e)}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            # Get counts from approval table
            approval_stats = self.spark.sql(f"""
                SELECT 
                    COUNT(*) as total_requests,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_requests,
                    SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved_requests,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_requests
                FROM {self.approval_table}
            """).collect()[0]
            
            # Get audit count
            audit_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.audit_table}").collect()[0]['count']
            
            return {
                "storage_mode": "unity_catalog",
                "catalog": self.catalog,
                "schema": self.schema,
                "total_requests": approval_stats['total_requests'],
                "pending_requests": approval_stats['pending_requests'],
                "approved_requests": approval_stats['approved_requests'],
                "completed_requests": approval_stats['completed_requests'],
                "audit_entries": audit_count,
                "last_updated": datetime.now().isoformat()
            }
        except Exception as e:
            return {"error": str(e)}
    
    def clear_all(self):
        """Clear all data (admin only)"""
        try:
            self.spark.sql(f"DELETE FROM {self.approval_table}")
            self.spark.sql(f"DELETE FROM {self.audit_table}")
            print("✅ All data cleared from Unity Catalog")
        except Exception as e:
            print(f"❌ Error clearing data: {str(e)}")
