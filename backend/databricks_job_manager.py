"""
Databricks Job Manager - Native Runtime Version
Optimized for Databricks Apps environment with runtime context
"""

import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs
    from databricks.sdk.runtime import *
    SDK_AVAILABLE = True
    DATABRICKS_RUNTIME = True
    print("✅ Using Databricks Runtime SDK")
except ImportError:
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service import jobs
        SDK_AVAILABLE = True
        DATABRICKS_RUNTIME = False
        print("✅ Using Databricks SDK (external)")
    except ImportError:
        SDK_AVAILABLE = False
        DATABRICKS_RUNTIME = False
        print("❌ Databricks SDK not available")

class DatabricksJobManager:
    """Native Databricks job manager using runtime context"""
    
    def __init__(self):
        if not SDK_AVAILABLE:
            raise RuntimeError("❌ Databricks SDK required for Databricks Apps")
        
        try:
            if DATABRICKS_RUNTIME:
                # Use Databricks runtime context
                self.client = WorkspaceClient()
                print("✅ Connected using Databricks runtime context")
            else:
                # Use configured profile or environment
                self.client = WorkspaceClient()
                print("✅ Connected using Databricks SDK")
                
        except Exception as e:
            raise RuntimeError(f"❌ Failed to initialize Databricks client: {str(e)}")
    
    def validate_connection(self) -> bool:
        """Validate connection to Databricks workspace"""
        try:
            # Test connection by listing a few jobs
            jobs_list = list(self.client.jobs.list(limit=1))
            print(f"✅ Connection validated - workspace accessible")
            return True
        except Exception as e:
            print(f"❌ Connection validation failed: {str(e)}")
            return False
    
    def list_jobs(self, limit: int = 100, name_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all jobs in the workspace"""
        try:
            jobs_list = []
            
            for job in self.client.jobs.list(limit=limit):
                job_info = {
                    'job_id': str(job.job_id),
                    'name': job.settings.name,
                    'created_time': job.created_time,
                    'creator_user_name': getattr(job, 'creator_user_name', 'Unknown'),
                    'job_type': self._determine_job_type(job.settings)
                }
                
                # Apply name filter if specified
                if name_filter is None or name_filter.lower() in job.settings.name.lower():
                    jobs_list.append(job_info)
            
            print(f"✅ Found {len(jobs_list)} jobs (filtered from workspace)")
            return jobs_list
            
        except Exception as e:
            print(f"❌ Error listing jobs: {str(e)}")
            return []
    
    def get_job_details(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific job"""
        try:
            job = self.client.jobs.get(job_id)
            
            details = {
                'job_id': str(job.job_id),
                'name': job.settings.name,
                'description': getattr(job.settings, 'description', ''),
                'created_time': job.created_time,
                'creator_user_name': getattr(job, 'creator_user_name', 'Unknown'),
                'job_type': self._determine_job_type(job.settings),
                'timeout_seconds': getattr(job.settings, 'timeout_seconds', None),
                'max_concurrent_runs': getattr(job.settings, 'max_concurrent_runs', 1),
                'tasks': self._extract_task_info(job.settings)
            }
            
            return details
            
        except Exception as e:
            print(f"❌ Error getting job details for {job_id}: {str(e)}")
            return None
    
    def trigger_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Trigger a job and return the run ID"""
        try:
            # Prepare run parameters
            run_params = {}
            
            if parameters:
                if 'notebook_params' in parameters:
                    run_params['notebook_params'] = parameters['notebook_params']
                if 'spark_submit_params' in parameters:
                    run_params['spark_submit_params'] = parameters['spark_submit_params']
                if 'python_params' in parameters:
                    run_params['python_params'] = parameters['python_params']
                if 'jar_params' in parameters:
                    run_params['jar_params'] = parameters['jar_params']
            
            # Trigger the job
            run = self.client.jobs.run_now(job_id=int(job_id), **run_params)
            run_id = str(run.run_id)
            
            print(f"✅ Job {job_id} triggered successfully - Run ID: {run_id}")
            return run_id
            
        except Exception as e:
            print(f"❌ Error triggering job {job_id}: {str(e)}")
            return None
    
    def get_run_status(self, run_id: str) -> Optional[str]:
        """Get the current status of a job run"""
        try:
            run = self.client.jobs.get_run(run_id=int(run_id))
            
            # Extract lifecycle state
            state = run.state
            if state.life_cycle_state:
                status = state.life_cycle_state.value.lower()
                
                # Map Databricks states to our simplified states
                if status in ['pending', 'running', 'terminating']:
                    return 'running'
                elif status == 'terminated':
                    if state.result_state:
                        if state.result_state.value == 'SUCCESS':
                            return 'completed'
                        else:
                            return 'failed'
                    return 'completed'  # Default for terminated
                elif status in ['skipped', 'internal_error']:
                    return 'failed'
                else:
                    return status
            
            return 'unknown'
            
        except Exception as e:
            print(f"❌ Error getting run status for {run_id}: {str(e)}")
            return None
    
    def get_run_details(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a job run"""
        try:
            run = self.client.jobs.get_run(run_id=int(run_id))
            
            details = {
                'run_id': str(run.run_id),
                'job_id': str(run.job_id),
                'run_name': getattr(run, 'run_name', ''),
                'creator_user_name': getattr(run, 'creator_user_name', 'Unknown'),
                'start_time': run.start_time,
                'end_time': getattr(run, 'end_time', None),
                'state': run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else 'unknown',
                'result_state': run.state.result_state.value if run.state and run.state.result_state else None,
                'state_message': getattr(run.state, 'state_message', '') if run.state else '',
                'run_duration': self._calculate_duration(run.start_time, getattr(run, 'end_time', None)),
                'cluster_instance': self._extract_cluster_info(run),
                'tasks': self._extract_run_task_info(run)
            }
            
            return details
            
        except Exception as e:
            print(f"❌ Error getting run details for {run_id}: {str(e)}")
            return None
    
    def cancel_run(self, run_id: str) -> bool:
        """Cancel a running job"""
        try:
            self.client.jobs.cancel_run(run_id=int(run_id))
            print(f"✅ Job run {run_id} cancelled successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error cancelling run {run_id}: {str(e)}")
            return False
    
    def get_recent_runs(self, job_id: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent job runs"""
        try:
            runs_list = []
            
            if job_id:
                # Get runs for specific job
                for run in self.client.jobs.list_runs(job_id=int(job_id), limit=limit):
                    runs_list.append(self._format_run_info(run))
            else:
                # Get all recent runs (this might be limited by API)
                for run in self.client.jobs.list_runs(limit=limit):
                    runs_list.append(self._format_run_info(run))
            
            return runs_list
            
        except Exception as e:
            print(f"❌ Error getting recent runs: {str(e)}")
            return []
    
    def wait_for_completion(self, run_id: str, timeout_minutes: int = 60, poll_interval: int = 30) -> str:
        """Wait for a job run to complete"""
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()
        
        print(f"⏳ Waiting for run {run_id} to complete (timeout: {timeout_minutes} min)")
        
        while time.time() - start_time < timeout_seconds:
            status = self.get_run_status(run_id)
            
            if status in ['completed', 'failed', 'cancelled']:
                print(f"✅ Run {run_id} finished with status: {status}")
                return status
            
            print(f"🔄 Run {run_id} status: {status} - waiting {poll_interval}s...")
            time.sleep(poll_interval)
        
        print(f"⚠️ Timeout waiting for run {run_id} to complete")
        return 'timeout'
    
    def _determine_job_type(self, settings) -> str:
        """Determine the type of job based on its settings"""
        if hasattr(settings, 'tasks') and settings.tasks:
            # Multi-task job
            task_types = []
            for task in settings.tasks:
                if hasattr(task, 'notebook_task') and task.notebook_task:
                    task_types.append('notebook')
                elif hasattr(task, 'spark_python_task') and task.spark_python_task:
                    task_types.append('python')
                elif hasattr(task, 'spark_submit_task') and task.spark_submit_task:
                    task_types.append('spark_submit')
                elif hasattr(task, 'spark_jar_task') and task.spark_jar_task:
                    task_types.append('jar')
                elif hasattr(task, 'python_wheel_task') and task.python_wheel_task:
                    task_types.append('wheel')
                else:
                    task_types.append('other')
            
            return f"multi_task ({', '.join(set(task_types))})"
        
        # Single task job (legacy)
        if hasattr(settings, 'notebook_task') and settings.notebook_task:
            return 'notebook'
        elif hasattr(settings, 'spark_python_task') and settings.spark_python_task:
            return 'python'
        elif hasattr(settings, 'spark_submit_task') and settings.spark_submit_task:
            return 'spark_submit'
        elif hasattr(settings, 'spark_jar_task') and settings.spark_jar_task:
            return 'jar'
        elif hasattr(settings, 'python_wheel_task') and settings.python_wheel_task:
            return 'wheel'
        else:
            return 'unknown'
    
    def _extract_task_info(self, settings) -> List[Dict[str, Any]]:
        """Extract task information from job settings"""
        tasks = []
        
        if hasattr(settings, 'tasks') and settings.tasks:
            for task in settings.tasks:
                task_info = {
                    'task_key': task.task_key,
                    'task_type': self._get_task_type(task),
                    'depends_on': [dep.task_key for dep in (task.depends_on or [])]
                }
                tasks.append(task_info)
        else:
            # Single task (legacy format)
            tasks.append({
                'task_key': 'main',
                'task_type': self._determine_job_type(settings),
                'depends_on': []
            })
        
        return tasks
    
    def _get_task_type(self, task) -> str:
        """Get the type of a specific task"""
        if hasattr(task, 'notebook_task') and task.notebook_task:
            return 'notebook'
        elif hasattr(task, 'spark_python_task') and task.spark_python_task:
            return 'python'
        elif hasattr(task, 'spark_submit_task') and task.spark_submit_task:
            return 'spark_submit'
        elif hasattr(task, 'spark_jar_task') and task.spark_jar_task:
            return 'jar'
        elif hasattr(task, 'python_wheel_task') and task.python_wheel_task:
            return 'wheel'
        else:
            return 'unknown'
    
    def _extract_cluster_info(self, run) -> Dict[str, Any]:
        """Extract cluster information from run"""
        cluster_info = {'type': 'unknown'}
        
        if hasattr(run, 'cluster_spec') and run.cluster_spec:
            if hasattr(run.cluster_spec, 'existing_cluster_id'):
                cluster_info = {
                    'type': 'existing',
                    'cluster_id': run.cluster_spec.existing_cluster_id
                }
            elif hasattr(run.cluster_spec, 'new_cluster'):
                cluster_info = {
                    'type': 'new',
                    'node_type': getattr(run.cluster_spec.new_cluster, 'node_type_id', 'unknown'),
                    'num_workers': getattr(run.cluster_spec.new_cluster, 'num_workers', 0)
                }
        
        return cluster_info
    
    def _extract_run_task_info(self, run) -> List[Dict[str, Any]]:
        """Extract task information from run"""
        tasks = []
        
        if hasattr(run, 'tasks') and run.tasks:
            for task in run.tasks:
                task_info = {
                    'task_key': task.task_key,
                    'state': task.state.life_cycle_state.value if task.state and task.state.life_cycle_state else 'unknown',
                    'start_time': getattr(task, 'start_time', None),
                    'end_time': getattr(task, 'end_time', None)
                }
                tasks.append(task_info)
        
        return tasks
    
    def _format_run_info(self, run) -> Dict[str, Any]:
        """Format run information for display"""
        return {
            'run_id': str(run.run_id),
            'job_id': str(run.job_id),
            'run_name': getattr(run, 'run_name', ''),
            'state': run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else 'unknown',
            'start_time': run.start_time,
            'end_time': getattr(run, 'end_time', None),
            'duration': self._calculate_duration(run.start_time, getattr(run, 'end_time', None))
        }
    
    def _calculate_duration(self, start_time, end_time) -> Optional[str]:
        """Calculate duration between start and end times"""
        if not start_time:
            return None
        
        if end_time:
            # Both times available
            duration_ms = end_time - start_time
            duration_seconds = duration_ms / 1000
            
            if duration_seconds < 60:
                return f"{duration_seconds:.1f}s"
            elif duration_seconds < 3600:
                return f"{duration_seconds/60:.1f}m"
            else:
                return f"{duration_seconds/3600:.1f}h"
        else:
            # Still running
            current_time = int(time.time() * 1000)
            duration_ms = current_time - start_time
            duration_seconds = duration_ms / 1000
            
            if duration_seconds < 60:
                return f"{duration_seconds:.1f}s (running)"
            elif duration_seconds < 3600:
                return f"{duration_seconds/60:.1f}m (running)"
            else:
                return f"{duration_seconds/3600:.1f}h (running)"


# Alias for backward compatibility
SDKJobManager = DatabricksJobManager
