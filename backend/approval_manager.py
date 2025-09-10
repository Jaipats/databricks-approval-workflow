"""
Approval Manager for Workflow Logic
Handles approval process, notifications, and workflow state transitions
"""

import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import json

class ApprovalStatus(Enum):
    """Approval status enumeration"""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"

class JobStatus(Enum):
    """Job execution status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class ApprovalManager:
    """Manages approval workflow logic and state transitions"""
    
    def __init__(self, approval_timeout_hours: int = 72):
        """
        Initialize ApprovalManager
        
        Args:
            approval_timeout_hours: Hours after which pending approvals expire
        """
        self.approval_timeout = timedelta(hours=approval_timeout_hours)
        self.notification_handlers = []
    
    def create_approval_request(self, 
                              requester_id: str,
                              job1_config: Dict[str, Any],
                              job2_config: Dict[str, Any],
                              metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Create a new approval request
        
        Args:
            requester_id: ID of the user requesting approval
            job1_config: Configuration for the first job
            job2_config: Configuration for the second job  
            metadata: Additional metadata for the request
            
        Returns:
            Unique request ID
        """
        request_id = str(uuid.uuid4())
        
        approval_data = {
            'request_id': request_id,
            'requester_id': requester_id,
            'job1_id': job1_config['job_id'],
            'job1_params': json.dumps(job1_config.get('params', {})),
            'job2_id': job2_config['job_id'],
            'job2_params': json.dumps(job2_config.get('params', {})),
            'status': ApprovalStatus.PENDING.value,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'metadata': json.dumps(metadata or {})
        }
        
        return request_id, approval_data
    
    def validate_approval_request(self, request_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate an approval request
        
        Args:
            request_data: The approval request data
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        required_fields = ['requester_id', 'job1_id', 'job2_id']
        
        for field in required_fields:
            if not request_data.get(field):
                return False, f"Missing required field: {field}"
        
        # Validate JSON parameters
        try:
            if request_data.get('job1_params'):
                json.loads(request_data['job1_params'])
            if request_data.get('job2_params'):
                json.loads(request_data['job2_params'])
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON in job parameters: {str(e)}"
        
        # Validate job IDs are different
        if request_data['job1_id'] == request_data['job2_id']:
            return False, "Job 1 and Job 2 must be different jobs"
        
        return True, ""
    
    def can_approve_request(self, request_data: Dict[str, Any], 
                          approver_id: str) -> Tuple[bool, str]:
        """
        Check if a request can be approved by the given user
        
        Args:
            request_data: The approval request data
            approver_id: ID of the potential approver
            
        Returns:
            Tuple of (can_approve, reason)
        """
        # Can't approve own requests
        if request_data.get('requester_id') == approver_id:
            return False, "Cannot approve your own request"
        
        # Check if request is in pending status
        if request_data.get('status') != ApprovalStatus.PENDING.value:
            return False, f"Request is not pending (current status: {request_data.get('status')})"
        
        # Check if request has expired
        if self.is_request_expired(request_data):
            return False, "Request has expired"
        
        # Check if Job 1 is completed successfully
        job1_status = request_data.get('job1_status')
        if job1_status != JobStatus.COMPLETED.value:
            return False, f"Job 1 must be completed before approval (current status: {job1_status})"
        
        return True, ""
    
    def is_request_expired(self, request_data: Dict[str, Any]) -> bool:
        """Check if an approval request has expired"""
        try:
            created_at = datetime.fromisoformat(request_data['created_at'].replace('Z', '+00:00'))
            return datetime.now() - created_at > self.approval_timeout
        except (KeyError, ValueError):
            return False
    
    def approve_request(self, request_id: str, approver_id: str, 
                       comments: Optional[str] = None) -> Dict[str, Any]:
        """
        Approve a request
        
        Args:
            request_id: The request ID to approve
            approver_id: ID of the approver
            comments: Optional approval comments
            
        Returns:
            Dictionary with approval update data
        """
        now = datetime.now()
        
        update_data = {
            'status': ApprovalStatus.APPROVED.value,
            'approver_id': approver_id,
            'approved_at': now.isoformat(),
            'updated_at': now.isoformat()
        }
        
        if comments:
            update_data['approval_comments'] = comments
        
        # Send notification
        self._send_notification(
            event_type="approval_granted",
            request_id=request_id,
            approver_id=approver_id,
            comments=comments
        )
        
        return update_data
    
    def reject_request(self, request_id: str, approver_id: str, 
                      reason: str) -> Dict[str, Any]:
        """
        Reject a request
        
        Args:
            request_id: The request ID to reject
            approver_id: ID of the approver
            reason: Reason for rejection
            
        Returns:
            Dictionary with rejection update data
        """
        now = datetime.now()
        
        update_data = {
            'status': ApprovalStatus.REJECTED.value,
            'approver_id': approver_id,
            'rejected_at': now.isoformat(),
            'rejection_reason': reason,
            'updated_at': now.isoformat()
        }
        
        # Send notification
        self._send_notification(
            event_type="approval_rejected",
            request_id=request_id,
            approver_id=approver_id,
            reason=reason
        )
        
        return update_data
    
    def update_job1_status(self, request_id: str, job1_run_id: str, 
                          status: str, details: Optional[str] = None) -> Dict[str, Any]:
        """
        Update Job 1 status
        
        Args:
            request_id: The request ID
            job1_run_id: The Job 1 run ID
            status: New status for Job 1
            details: Optional status details
            
        Returns:
            Dictionary with update data
        """
        update_data = {
            'job1_run_id': job1_run_id,
            'job1_status': status,
            'updated_at': datetime.now().isoformat()
        }
        
        if details:
            update_data['job1_details'] = details
        
        # If Job 1 completed successfully, send notification to approvers
        if status == JobStatus.COMPLETED.value:
            self._send_notification(
                event_type="job1_completed",
                request_id=request_id,
                job_run_id=job1_run_id
            )
        elif status == JobStatus.FAILED.value:
            self._send_notification(
                event_type="job1_failed",
                request_id=request_id,
                job_run_id=job1_run_id,
                details=details
            )
        
        return update_data
    
    def update_job2_status(self, request_id: str, job2_run_id: str, 
                          status: str, details: Optional[str] = None) -> Dict[str, Any]:
        """
        Update Job 2 status
        
        Args:
            request_id: The request ID
            job2_run_id: The Job 2 run ID
            status: New status for Job 2
            details: Optional status details
            
        Returns:
            Dictionary with update data
        """
        update_data = {
            'job2_run_id': job2_run_id,
            'job2_status': status,
            'updated_at': datetime.now().isoformat()
        }
        
        if details:
            update_data['job2_details'] = details
        
        # If Job 2 completed, mark the entire workflow as completed
        if status == JobStatus.COMPLETED.value:
            update_data['status'] = ApprovalStatus.COMPLETED.value
            update_data['completed_at'] = datetime.now().isoformat()
            
            self._send_notification(
                event_type="workflow_completed",
                request_id=request_id,
                job_run_id=job2_run_id
            )
        elif status == JobStatus.FAILED.value:
            self._send_notification(
                event_type="job2_failed",
                request_id=request_id,
                job_run_id=job2_run_id,
                details=details
            )
        
        return update_data
    
    def get_workflow_status(self, request_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Get comprehensive workflow status
        
        Args:
            request_data: The approval request data
            
        Returns:
            Dictionary with workflow status information
        """
        status_info = {
            'workflow_status': request_data.get('status', 'unknown'),
            'job1_status': request_data.get('job1_status', 'pending'),
            'job2_status': request_data.get('job2_status', 'pending'),
            'current_stage': self._determine_current_stage(request_data),
            'next_action': self._determine_next_action(request_data)
        }
        
        # Add timing information
        created_at = request_data.get('created_at')
        if created_at:
            status_info['created_at'] = created_at
            status_info['age_hours'] = self._calculate_age_hours(created_at)
        
        approved_at = request_data.get('approved_at')
        if approved_at:
            status_info['approved_at'] = approved_at
        
        completed_at = request_data.get('completed_at')
        if completed_at:
            status_info['completed_at'] = completed_at
        
        return status_info
    
    def _determine_current_stage(self, request_data: Dict[str, Any]) -> str:
        """Determine the current stage of the workflow"""
        workflow_status = request_data.get('status')
        job1_status = request_data.get('job1_status')
        job2_status = request_data.get('job2_status')
        
        if workflow_status == ApprovalStatus.COMPLETED.value:
            return "completed"
        elif workflow_status == ApprovalStatus.REJECTED.value:
            return "rejected"
        elif workflow_status == ApprovalStatus.CANCELLED.value:
            return "cancelled"
        elif job2_status == JobStatus.RUNNING.value:
            return "job2_running"
        elif workflow_status == ApprovalStatus.APPROVED.value:
            return "approved_awaiting_job2"
        elif job1_status == JobStatus.COMPLETED.value:
            return "awaiting_approval"
        elif job1_status == JobStatus.RUNNING.value:
            return "job1_running"
        elif job1_status == JobStatus.FAILED.value:
            return "job1_failed"
        else:
            return "job1_pending"
    
    def _determine_next_action(self, request_data: Dict[str, Any]) -> str:
        """Determine the next required action"""
        stage = self._determine_current_stage(request_data)
        
        action_map = {
            "job1_pending": "Start Job 1",
            "job1_running": "Wait for Job 1 completion",
            "job1_failed": "Fix and restart Job 1",
            "awaiting_approval": "Requires approval",
            "approved_awaiting_job2": "Start Job 2",
            "job2_running": "Wait for Job 2 completion",
            "completed": "Workflow complete",
            "rejected": "Request was rejected",
            "cancelled": "Request was cancelled"
        }
        
        return action_map.get(stage, "Unknown")
    
    def _calculate_age_hours(self, created_at: str) -> float:
        """Calculate the age of a request in hours"""
        try:
            created_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            age = datetime.now() - created_time
            return round(age.total_seconds() / 3600, 1)
        except (ValueError, AttributeError):
            return 0.0
    
    def expire_old_requests(self, requests: List[Dict[str, Any]]) -> List[str]:
        """
        Mark old pending requests as expired
        
        Args:
            requests: List of approval request data
            
        Returns:
            List of request IDs that were expired
        """
        expired_ids = []
        
        for request in requests:
            if (request.get('status') == ApprovalStatus.PENDING.value and 
                self.is_request_expired(request)):
                expired_ids.append(request['request_id'])
        
        return expired_ids
    
    def add_notification_handler(self, handler):
        """Add a notification handler function"""
        self.notification_handlers.append(handler)
    
    def _send_notification(self, event_type: str, request_id: str, **kwargs):
        """Send notifications to all registered handlers"""
        notification_data = {
            'event_type': event_type,
            'request_id': request_id,
            'timestamp': datetime.now().isoformat(),
            **kwargs
        }
        
        for handler in self.notification_handlers:
            try:
                handler(notification_data)
            except Exception as e:
                print(f"⚠️ Notification handler error: {str(e)}")
    
    def get_approval_metrics(self, requests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate approval workflow metrics
        
        Args:
            requests: List of all approval requests
            
        Returns:
            Dictionary with metrics
        """
        if not requests:
            return {
                'total_requests': 0,
                'pending_requests': 0,
                'approved_requests': 0,
                'rejected_requests': 0,
                'completed_workflows': 0,
                'average_approval_time_hours': 0.0,
                'success_rate': 0.0
            }
        
        total = len(requests)
        pending = sum(1 for r in requests if r.get('status') == ApprovalStatus.PENDING.value)
        approved = sum(1 for r in requests if r.get('status') == ApprovalStatus.APPROVED.value)
        rejected = sum(1 for r in requests if r.get('status') == ApprovalStatus.REJECTED.value)
        completed = sum(1 for r in requests if r.get('status') == ApprovalStatus.COMPLETED.value)
        
        # Calculate average approval time
        approval_times = []
        for request in requests:
            if request.get('approved_at') and request.get('created_at'):
                try:
                    created = datetime.fromisoformat(request['created_at'].replace('Z', '+00:00'))
                    approved_time = datetime.fromisoformat(request['approved_at'].replace('Z', '+00:00'))
                    hours = (approved_time - created).total_seconds() / 3600
                    approval_times.append(hours)
                except (ValueError, AttributeError):
                    continue
        
        avg_approval_time = sum(approval_times) / len(approval_times) if approval_times else 0.0
        success_rate = (completed / total * 100) if total > 0 else 0.0
        
        return {
            'total_requests': total,
            'pending_requests': pending,
            'approved_requests': approved,
            'rejected_requests': rejected,
            'completed_workflows': completed,
            'average_approval_time_hours': round(avg_approval_time, 1),
            'success_rate': round(success_rate, 1)
        }


class NotificationService:
    """Service for handling notifications (email, Slack, etc.)"""
    
    def __init__(self):
        self.handlers = {
            'console': self._console_handler,
            'email': self._email_handler,
            'slack': self._slack_handler
        }
    
    def _console_handler(self, notification: Dict[str, Any]):
        """Simple console notification handler"""
        event_type = notification.get('event_type')
        request_id = notification.get('request_id', 'unknown')[:8]
        
        messages = {
            'approval_granted': f"✅ Request {request_id} approved",
            'approval_rejected': f"❌ Request {request_id} rejected: {notification.get('reason', 'No reason provided')}",
            'job1_completed': f"🎯 Job 1 completed for request {request_id}",
            'job1_failed': f"💥 Job 1 failed for request {request_id}",
            'workflow_completed': f"🎉 Workflow {request_id} completed successfully",
            'job2_failed': f"💥 Job 2 failed for request {request_id}"
        }
        
        message = messages.get(event_type, f"📧 Notification: {event_type} for {request_id}")
        print(f"[NOTIFICATION] {message}")
    
    def _email_handler(self, notification: Dict[str, Any]):
        """Email notification handler (placeholder)"""
        # Implement email sending logic here
        print(f"[EMAIL] Would send email notification: {notification}")
    
    def _slack_handler(self, notification: Dict[str, Any]):
        """Slack notification handler (placeholder)"""
        # Implement Slack webhook logic here
        print(f"[SLACK] Would send Slack notification: {notification}")
    
    def send_notification(self, notification: Dict[str, Any], 
                         handlers: List[str] = None):
        """Send notification using specified handlers"""
        if handlers is None:
            handlers = ['console']
        
        for handler_name in handlers:
            if handler_name in self.handlers:
                self.handlers[handler_name](notification)
            else:
                print(f"⚠️ Unknown notification handler: {handler_name}")
