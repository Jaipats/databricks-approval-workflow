"""
Databricks SDK Job Manager
Handles job operations using the official Databricks SDK
"""

import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import logging

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import RunNowResponse, BaseRun, RunState, RunLifeCycleState, RunResultState
    from databricks.sdk.core import DatabricksError
    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False
    print("⚠️ Databricks SDK not available. Install with: pip install databricks-sdk")

class SDKJobManager:
    """Manages Databricks jobs using the official Python SDK"""
    
    def __init__(self, profile: Optional[str] = None):
        """
        Initialize SDK Job Manager
        
        Args:
            profile: Databricks CLI profile name (uses DEFAULT if None)
        """
        if not DATABRICKS_SDK_AVAILABLE:
            raise ImportError("Databricks SDK is required. Install with: pip install databricks-sdk")
        
        try:
            # Initialize workspace client using CLI profile
            self.workspace = WorkspaceClient(profile=profile)
            
            # Test connection
            current_user = self.workspace.current_user.me()
            print(f"✅ Connected to Databricks as: {current_user.user_name}")
            
        except Exception as e:
            print(f"❌ Failed to initialize Databricks SDK: {str(e)}")
            raise
    
    def list_jobs(self, limit: int = 50, name_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all jobs in the workspace
        
        Args:
            limit: Maximum number of jobs to return
            name_filter: Optional name filter
            
        Returns:
            List of job information dictionaries
        """
        try:
            jobs = []
            job_list = self.workspace.jobs.list(limit=limit, expand_tasks=False)
            
            for job in job_list:
                job_info = {
                    'job_id': str(job.job_id),
                    'name': job.settings.name if job.settings else f"Job {job.job_id}",
                    'creator_user_name': job.creator_user_name or "Unknown",
                    'created_time': job.created_time,
                    'timeout_seconds': job.settings.timeout_seconds if job.settings else None,
                    'max_concurrent_runs': job.settings.max_concurrent_runs if job.settings else 1
                }
                
                # Apply name filter if provided
                if name_filter and name_filter.lower() not in job_info['name'].lower():
                    continue
                    
                jobs.append(job_info)
            
            print(f"✅ Found {len(jobs)} jobs")
            return jobs
            
        except Exception as e:
            print(f"❌ Error listing jobs: {str(e)}")
            return []
    
    def get_job_details(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific job
        
        Args:
            job_id: The job ID to get details for
            
        Returns:
            Job details dictionary or None if not found
        """
        try:
            job = self.workspace.jobs.get(job_id=int(job_id))
            
            job_details = {
                'job_id': str(job.job_id),
                'name': job.settings.name if job.settings else f"Job {job.job_id}",
                'creator_user_name': job.creator_user_name or "Unknown",
                'created_time': job.created_time,
                'timeout_seconds': job.settings.timeout_seconds if job.settings else None,
                'max_concurrent_runs': job.settings.max_concurrent_runs if job.settings else 1,
                'tasks': []
            }
            
            # Add task information
            if job.settings and job.settings.tasks:
                for task in job.settings.tasks:
                    task_info = {
                        'task_key': task.task_key,
                        'description': task.description or "",
                        'timeout_seconds': task.timeout_seconds,
                        'max_retries': task.max_retries or 0
                    }
                    
                    # Add task type information
                    if task.notebook_task:
                        task_info['type'] = 'notebook'
                        task_info['notebook_path'] = task.notebook_task.notebook_path
                    elif task.python_wheel_task:
                        task_info['type'] = 'python_wheel'
                    elif task.jar_task:
                        task_info['type'] = 'jar'
                    elif task.spark_submit_task:
                        task_info['type'] = 'spark_submit'
                    else:
                        task_info['type'] = 'unknown'
                    
                    job_details['tasks'].append(task_info)
            
            return job_details
            
        except Exception as e:
            print(f"❌ Error getting job details for {job_id}: {str(e)}")
            return None
    
    def trigger_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        Trigger a job run
        
        Args:
            job_id: The job ID to trigger
            parameters: Optional parameters for the job
            
        Returns:
            Run ID if successful, None if failed
        """
        try:
            print(f"🎯 Triggering job {job_id}...")
            
            # Prepare run parameters
            run_params = {}
            
            if parameters:
                # Handle different parameter types
                if 'notebook_params' in parameters:
                    run_params['notebook_params'] = parameters['notebook_params']
                
                if 'jar_params' in parameters:
                    run_params['jar_params'] = parameters['jar_params']
                
                if 'python_params' in parameters:
                    run_params['python_params'] = parameters['python_params']
                
                if 'spark_submit_params' in parameters:
                    run_params['spark_submit_params'] = parameters['spark_submit_params']
            
            # Trigger the job
            response = self.workspace.jobs.run_now(job_id=int(job_id), **run_params)
            
            if response and response.run_id:
                run_id = str(response.run_id)
                print(f"✅ Job {job_id} triggered successfully. Run ID: {run_id}")
                return run_id
            else:
                print(f"❌ Failed to trigger job {job_id} - no run ID returned")
                return None
                
        except DatabricksError as e:
            print(f"❌ Databricks error triggering job {job_id}: {str(e)}")
            return None
        except Exception as e:
            print(f"❌ Error triggering job {job_id}: {str(e)}")
            return None
    
    def get_run_status(self, run_id: str) -> Optional[str]:
        """
        Get the current status of a job run
        
        Args:
            run_id: The run ID to check
            
        Returns:
            Status string: 'pending', 'running', 'completed', 'failed', 'cancelled'
        """
        try:
            run = self.workspace.jobs.get_run(run_id=int(run_id))
            
            if not run.state:
                return 'unknown'
            
            # Map SDK states to our simplified states
            life_cycle_state = run.state.life_cycle_state
            result_state = run.state.result_state
            
            if life_cycle_state in [RunLifeCycleState.PENDING, RunLifeCycleState.RUNNING, RunLifeCycleState.TERMINATING]:
                return 'running'
            elif life_cycle_state == RunLifeCycleState.TERMINATED:
                if result_state == RunResultState.SUCCESS:
                    return 'completed'
                elif result_state == RunResultState.FAILED:
                    return 'failed'
                elif result_state == RunResultState.CANCELED:
                    return 'cancelled'
                else:
                    return 'failed'  # Default to failed for other terminated states
            elif life_cycle_state in [RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                return 'failed'
            else:
                return 'unknown'
                
        except Exception as e:
            print(f"❌ Error getting run status for {run_id}: {str(e)}")
            return None
    
    def get_run_details(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a job run
        
        Args:
            run_id: The run ID to get details for
            
        Returns:
            Run details dictionary or None if not found
        """
        try:
            run = self.workspace.jobs.get_run(run_id=int(run_id))
            
            run_details = {
                'run_id': str(run.run_id),
                'job_id': str(run.job_id),
                'run_name': run.run_name or f"Run {run.run_id}",
                'state': {
                    'life_cycle_state': run.state.life_cycle_state.value if run.state else None,
                    'result_state': run.state.result_state.value if run.state and run.state.result_state else None,
                    'state_message': run.state.state_message if run.state else None
                },
                'start_time': run.start_time,
                'end_time': run.end_time,
                'setup_duration': run.setup_duration,
                'execution_duration': run.execution_duration,
                'cleanup_duration': run.cleanup_duration,
                'creator_user_name': run.creator_user_name or "Unknown"
            }
            
            # Add task run information
            if run.tasks:
                run_details['tasks'] = []
                for task in run.tasks:
                    task_info = {
                        'run_id': str(task.run_id),
                        'task_key': task.task_key,
                        'state': {
                            'life_cycle_state': task.state.life_cycle_state.value if task.state else None,
                            'result_state': task.state.result_state.value if task.state and task.state.result_state else None
                        },
                        'start_time': task.start_time,
                        'end_time': task.end_time,
                        'execution_duration': task.execution_duration
                    }
                    run_details['tasks'].append(task_info)
            
            return run_details
            
        except Exception as e:
            print(f"❌ Error getting run details for {run_id}: {str(e)}")
            return None
    
    def cancel_run(self, run_id: str) -> bool:
        """
        Cancel a running job
        
        Args:
            run_id: The run ID to cancel
            
        Returns:
            True if successfully cancelled, False otherwise
        """
        try:
            self.workspace.jobs.cancel_run(run_id=int(run_id))
            print(f"✅ Job run {run_id} cancelled successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error cancelling run {run_id}: {str(e)}")
            return False
    
    def wait_for_completion(self, run_id: str, timeout_minutes: int = 60, 
                           poll_interval_seconds: int = 30) -> Tuple[str, Optional[Dict]]:
        """
        Wait for a job run to complete
        
        Args:
            run_id: The run ID to wait for
            timeout_minutes: Maximum time to wait in minutes
            poll_interval_seconds: How often to check status
            
        Returns:
            Tuple of (final_status, run_details)
        """
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        print(f"⏳ Waiting for run {run_id} to complete (timeout: {timeout_minutes}m)...")
        
        while True:
            status = self.get_run_status(run_id)
            
            if status in ['completed', 'failed', 'cancelled']:
                print(f"🏁 Run {run_id} finished with status: {status}")
                run_details = self.get_run_details(run_id)
                return status, run_details
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                print(f"⏰ Timeout waiting for run {run_id}")
                return 'timeout', None
            
            print(f"🔄 Run {run_id} status: {status}, waiting {poll_interval_seconds}s...")
            time.sleep(poll_interval_seconds)
    
    def list_recent_runs(self, job_id: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """
        List recent job runs
        
        Args:
            job_id: Optional job ID to filter by
            limit: Maximum number of runs to return
            
        Returns:
            List of run information dictionaries
        """
        try:
            runs = []
            
            if job_id:
                # Get runs for specific job
                run_list = self.workspace.jobs.list_runs(job_id=int(job_id), limit=limit)
            else:
                # Get all recent runs
                run_list = self.workspace.jobs.list_runs(limit=limit)
            
            for run in run_list:
                run_info = {
                    'run_id': str(run.run_id),
                    'job_id': str(run.job_id),
                    'run_name': run.run_name or f"Run {run.run_id}",
                    'state': {
                        'life_cycle_state': run.state.life_cycle_state.value if run.state else None,
                        'result_state': run.state.result_state.value if run.state and run.state.result_state else None
                    },
                    'start_time': run.start_time,
                    'end_time': run.end_time,
                    'creator_user_name': run.creator_user_name or "Unknown"
                }
                runs.append(run_info)
            
            return runs
            
        except Exception as e:
            print(f"❌ Error listing runs: {str(e)}")
            return []
    
    def validate_connection(self) -> bool:
        """Test if the SDK connection is working"""
        try:
            # Try to get current user info as a connection test
            current_user = self.workspace.current_user.me()
            return bool(current_user.user_name)
            
        except Exception as e:
            print(f"❌ Connection validation failed: {str(e)}")
            return False
    
    def get_job_run_output(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the output of a job run
        
        Args:
            run_id: The run ID to get output for
            
        Returns:
            Output dictionary or None if not available
        """
        try:
            output = self.workspace.jobs.get_run_output(run_id=int(run_id))
            
            if output:
                return {
                    'notebook_output': output.notebook_output.result if output.notebook_output else None,
                    'error': output.error if output.error else None,
                    'metadata': output.metadata if output.metadata else None
                }
            
            return None
            
        except Exception as e:
            print(f"❌ Error getting run output for {run_id}: {str(e)}")
            return None


class MockSDKJobManager:
    """Mock SDK Job Manager for testing without Databricks connection"""
    
    def __init__(self, profile: Optional[str] = None):
        """Initialize mock manager"""
        self.mock_jobs = {
            "72987522543057": {
                "job_id": "72987522543057", 
                "name": "Data Processing Workflow - First Job", 
                "creator_user_name": "user@company.com",
                "tasks": [{"task_key": "process_data", "type": "notebook"}]
            },
            "883505719297722": {
                "job_id": "883505719297722", 
                "name": "ML Training Pipeline - Second Job", 
                "creator_user_name": "user@company.com",
                "tasks": [{"task_key": "train_model", "type": "notebook"}]
            }
        }
        self.mock_runs = {}
        self.run_counter = 1000
        print("✅ Mock SDK Job Manager initialized")
    
    def list_jobs(self, limit: int = 50, name_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Mock job listing"""
        jobs = list(self.mock_jobs.values())[:limit]
        if name_filter:
            jobs = [job for job in jobs if name_filter.lower() in job['name'].lower()]
        return jobs
    
    def get_job_details(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Mock job details"""
        return self.mock_jobs.get(job_id)
    
    def trigger_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Mock job triggering"""
        if job_id not in self.mock_jobs:
            return None
        
        run_id = str(self.run_counter)
        self.run_counter += 1
        
        self.mock_runs[run_id] = {
            "run_id": run_id,
            "job_id": job_id,
            "status": "running",
            "start_time": datetime.now().isoformat(),
            "parameters": parameters or {}
        }
        
        print(f"✅ Mock job {job_id} triggered. Run ID: {run_id}")
        return run_id
    
    def get_run_status(self, run_id: str) -> Optional[str]:
        """Mock run status - automatically complete after creation"""
        run = self.mock_runs.get(run_id)
        if not run:
            return None
        
        # Auto-complete for demo purposes
        if run["status"] == "running":
            run["status"] = "completed"
        
        return run["status"]
    
    def validate_connection(self) -> bool:
        """Mock connection validation"""
        return True
    
    def cancel_run(self, run_id: str) -> bool:
        """Mock job cancellation"""
        if run_id in self.mock_runs:
            self.mock_runs[run_id]["status"] = "cancelled"
            return True
        return False
    
    def get_run_details(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Mock run details"""
        run = self.mock_runs.get(run_id)
        if not run:
            return None
        
        return {
            'run_id': run_id,
            'job_id': run['job_id'],
            'run_name': f"Mock Run {run_id}",
            'state': {
                'life_cycle_state': 'TERMINATED',
                'result_state': 'SUCCESS',
                'state_message': 'Mock run completed successfully'
            },
            'start_time': run['start_time'],
            'end_time': datetime.now().isoformat(),
            'creator_user_name': 'mock_user@company.com'
        }
