"""
Job Manager for Databricks Jobs API Integration
Handles triggering and monitoring Databricks jobs
"""

import requests
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

class JobManager:
    """Manages Databricks Jobs API interactions"""
    
    def __init__(self, databricks_host: str, access_token: str):
        """
        Initialize JobManager
        
        Args:
            databricks_host: Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
            access_token: Databricks personal access token or service principal token
        """
        self.host = databricks_host.rstrip('/')
        self.token = access_token
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
        # API endpoints
        self.jobs_api = f"{self.host}/api/2.1/jobs"
        self.runs_api = f"{self.host}/api/2.1/jobs/runs"
    
    def _make_request(self, method: str, url: str, data: Optional[Dict] = None) -> Dict:
        """Make HTTP request to Databricks API"""
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=self.headers, params=data)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=self.headers, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed: {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"Response: {e.response.text}")
            return {"error": str(e)}
    
    def list_jobs(self, limit: int = 25) -> List[Dict]:
        """List all jobs in the workspace"""
        try:
            url = f"{self.jobs_api}/list"
            params = {"limit": limit, "expand_tasks": False}
            
            response = self._make_request('GET', url, params)
            
            if "error" in response:
                return []
            
            return response.get("jobs", [])
            
        except Exception as e:
            print(f"❌ Error listing jobs: {str(e)}")
            return []
    
    def get_job_details(self, job_id: str) -> Optional[Dict]:
        """Get detailed information about a specific job"""
        try:
            url = f"{self.jobs_api}/get"
            params = {"job_id": job_id}
            
            response = self._make_request('GET', url, params)
            
            if "error" in response:
                return None
            
            return response
            
        except Exception as e:
            print(f"❌ Error getting job details for {job_id}: {str(e)}")
            return None
    
    def trigger_job(self, job_id: str, parameters: Optional[Dict] = None, 
                   notebook_params: Optional[Dict] = None,
                   jar_params: Optional[List] = None,
                   python_params: Optional[List] = None) -> Optional[str]:
        """
        Trigger a Databricks job run
        
        Args:
            job_id: The ID of the job to trigger
            parameters: Job-level parameters
            notebook_params: Notebook parameters for notebook jobs
            jar_params: JAR parameters for JAR jobs  
            python_params: Python parameters for Python jobs
            
        Returns:
            Run ID if successful, None if failed
        """
        try:
            url = f"{self.runs_api}/run-now"
            
            payload = {
                "job_id": int(job_id)
            }
            
            # Add parameters if provided
            if parameters:
                payload.update(parameters)
            
            if notebook_params:
                payload["notebook_params"] = notebook_params
            
            if jar_params:
                payload["jar_params"] = jar_params
                
            if python_params:
                payload["python_params"] = python_params
            
            response = self._make_request('POST', url, payload)
            
            if "error" in response:
                print(f"❌ Failed to trigger job {job_id}: {response['error']}")
                return None
            
            run_id = response.get("run_id")
            print(f"✅ Job {job_id} triggered successfully. Run ID: {run_id}")
            
            return str(run_id)
            
        except Exception as e:
            print(f"❌ Error triggering job {job_id}: {str(e)}")
            return None
    
    def get_job_run_status(self, run_id: str) -> Optional[str]:
        """Get the current status of a job run"""
        try:
            url = f"{self.runs_api}/get"
            params = {"run_id": run_id}
            
            response = self._make_request('GET', url, params)
            
            if "error" in response:
                return None
            
            state = response.get("state", {})
            life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
            result_state = state.get("result_state")
            
            # Map Databricks states to simplified states
            if life_cycle_state in ["PENDING", "RUNNING"]:
                return "running"
            elif life_cycle_state == "TERMINATED":
                if result_state == "SUCCESS":
                    return "completed"
                else:
                    return "failed"
            elif life_cycle_state in ["SKIPPED", "INTERNAL_ERROR"]:
                return "failed"
            else:
                return "unknown"
                
        except Exception as e:
            print(f"❌ Error getting run status for {run_id}: {str(e)}")
            return None
    
    def get_job_run_details(self, run_id: str) -> Optional[Dict]:
        """Get detailed information about a job run"""
        try:
            url = f"{self.runs_api}/get"
            params = {"run_id": run_id, "include_history": True}
            
            response = self._make_request('GET', url, params)
            
            if "error" in response:
                return None
            
            return response
            
        except Exception as e:
            print(f"❌ Error getting run details for {run_id}: {str(e)}")
            return None
    
    def cancel_job_run(self, run_id: str) -> bool:
        """Cancel a running job"""
        try:
            url = f"{self.runs_api}/cancel"
            payload = {"run_id": int(run_id)}
            
            response = self._make_request('POST', url, payload)
            
            if "error" in response:
                print(f"❌ Failed to cancel run {run_id}: {response['error']}")
                return False
            
            print(f"✅ Job run {run_id} cancelled successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error cancelling run {run_id}: {str(e)}")
            return False
    
    def list_job_runs(self, job_id: Optional[str] = None, 
                     active_only: bool = False, limit: int = 25) -> List[Dict]:
        """List job runs, optionally filtered by job ID"""
        try:
            url = f"{self.runs_api}/list"
            params = {
                "limit": limit,
                "expand_tasks": False
            }
            
            if job_id:
                params["job_id"] = job_id
            
            if active_only:
                params["active_only"] = True
            
            response = self._make_request('GET', url, params)
            
            if "error" in response:
                return []
            
            return response.get("runs", [])
            
        except Exception as e:
            print(f"❌ Error listing job runs: {str(e)}")
            return []
    
    def wait_for_job_completion(self, run_id: str, timeout_minutes: int = 60, 
                               poll_interval_seconds: int = 30) -> str:
        """
        Wait for a job run to complete
        
        Args:
            run_id: The run ID to wait for
            timeout_minutes: Maximum time to wait in minutes
            poll_interval_seconds: How often to check status
            
        Returns:
            Final status: 'completed', 'failed', or 'timeout'
        """
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        while True:
            status = self.get_job_run_status(run_id)
            
            if status in ['completed', 'failed']:
                return status
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                print(f"⏰ Timeout waiting for job run {run_id}")
                return 'timeout'
            
            # Wait before next poll
            time.sleep(poll_interval_seconds)
    
    def get_job_run_output(self, run_id: str) -> Optional[Dict]:
        """Get the output of a job run"""
        try:
            url = f"{self.runs_api}/get-output"
            params = {"run_id": run_id}
            
            response = self._make_request('GET', url, params)
            
            if "error" in response:
                return None
            
            return response
            
        except Exception as e:
            print(f"❌ Error getting run output for {run_id}: {str(e)}")
            return None
    
    def create_job(self, job_config: Dict) -> Optional[str]:
        """Create a new job (for testing purposes)"""
        try:
            url = f"{self.jobs_api}/create"
            
            response = self._make_request('POST', url, job_config)
            
            if "error" in response:
                print(f"❌ Failed to create job: {response['error']}")
                return None
            
            job_id = response.get("job_id")
            print(f"✅ Job created successfully. Job ID: {job_id}")
            
            return str(job_id)
            
        except Exception as e:
            print(f"❌ Error creating job: {str(e)}")
            return None
    
    def validate_connection(self) -> bool:
        """Test if the connection to Databricks is working"""
        try:
            # Try to list jobs as a connection test
            jobs = self.list_jobs(limit=1)
            return isinstance(jobs, list)  # Even empty list means connection works
            
        except Exception as e:
            print(f"❌ Connection validation failed: {str(e)}")
            return False
    
    def get_cluster_info(self, cluster_id: str) -> Optional[Dict]:
        """Get information about a cluster (if needed for job dependencies)"""
        try:
            url = f"{self.host}/api/2.0/clusters/get"
            params = {"cluster_id": cluster_id}
            
            response = self._make_request('GET', url, params)
            
            if "error" in response:
                return None
            
            return response
            
        except Exception as e:
            print(f"❌ Error getting cluster info for {cluster_id}: {str(e)}")
            return None
    
    def get_job_permissions(self, job_id: str) -> Optional[Dict]:
        """Get permissions for a job"""
        try:
            url = f"{self.host}/api/2.0/permissions/jobs/{job_id}"
            
            response = self._make_request('GET', url)
            
            if "error" in response:
                return None
            
            return response
            
        except Exception as e:
            print(f"❌ Error getting job permissions for {job_id}: {str(e)}")
            return None


class MockJobManager(JobManager):
    """Mock Job Manager for testing without actual Databricks connection"""
    
    def __init__(self):
        # Don't call parent __init__ to avoid requiring credentials
        self.mock_jobs = {
            "12345": {"job_id": "12345", "name": "Data Processing Job", "creator_user_name": "test@example.com"},
            "67890": {"job_id": "67890", "name": "ML Training Job", "creator_user_name": "test@example.com"}
        }
        self.mock_runs = {}
        self.run_counter = 1000
    
    def list_jobs(self, limit: int = 25) -> List[Dict]:
        """Mock job listing"""
        return list(self.mock_jobs.values())[:limit]
    
    def get_job_details(self, job_id: str) -> Optional[Dict]:
        """Mock job details"""
        return self.mock_jobs.get(job_id)
    
    def trigger_job(self, job_id: str, parameters: Optional[Dict] = None, 
                   **kwargs) -> Optional[str]:
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
        
        return run_id
    
    def get_job_run_status(self, run_id: str) -> Optional[str]:
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
    
    def cancel_job_run(self, run_id: str) -> bool:
        """Mock job cancellation"""
        if run_id in self.mock_runs:
            self.mock_runs[run_id]["status"] = "cancelled"
            return True
        return False
