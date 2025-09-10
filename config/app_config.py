"""
Configuration settings for Databricks Approval App
"""

import os
from typing import Dict, Any

class AppConfig:
    """Main application configuration"""
    
    # Databricks Configuration
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://your-workspace.cloud.databricks.com")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
    
    # Database Configuration
    CATALOG_NAME = os.getenv("CATALOG_NAME", "main")
    SCHEMA_NAME = os.getenv("SCHEMA_NAME", "approval_workflow")
    
    # App Configuration
    APP_TITLE = "Databricks Job Approval Workflow"
    APP_ICON = "🚀"
    PAGE_LAYOUT = "wide"
    
    # Approval Workflow Settings
    APPROVAL_TIMEOUT_HOURS = int(os.getenv("APPROVAL_TIMEOUT_HOURS", "72"))
    MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "10"))
    JOB_POLL_INTERVAL_SECONDS = int(os.getenv("JOB_POLL_INTERVAL_SECONDS", "30"))
    
    # Authentication Settings
    ENABLE_AUTHENTICATION = os.getenv("ENABLE_AUTHENTICATION", "false").lower() == "true"
    ADMIN_USERS = os.getenv("ADMIN_USERS", "admin@company.com").split(",")
    
    # Notification Settings
    ENABLE_NOTIFICATIONS = os.getenv("ENABLE_NOTIFICATIONS", "true").lower() == "true"
    NOTIFICATION_HANDLERS = os.getenv("NOTIFICATION_HANDLERS", "console").split(",")
    
    # Slack Configuration (if using Slack notifications)
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
    SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#databricks-jobs")
    
    # Email Configuration (if using email notifications)
    SMTP_SERVER = os.getenv("SMTP_SERVER", "")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    EMAIL_FROM = os.getenv("EMAIL_FROM", "")
    
    # Development Settings
    DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
    USE_MOCK_JOBS = os.getenv("USE_MOCK_JOBS", "false").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    @classmethod
    def get_databricks_config(cls) -> Dict[str, str]:
        """Get Databricks configuration"""
        return {
            "host": cls.DATABRICKS_HOST,
            "token": cls.DATABRICKS_TOKEN
        }
    
    @classmethod
    def get_database_config(cls) -> Dict[str, str]:
        """Get database configuration"""
        return {
            "catalog": cls.CATALOG_NAME,
            "schema": cls.SCHEMA_NAME
        }
    
    @classmethod
    def validate_config(cls) -> Dict[str, bool]:
        """Validate configuration and return status"""
        validation = {
            "databricks_host_set": bool(cls.DATABRICKS_HOST and cls.DATABRICKS_HOST != "https://your-workspace.cloud.databricks.com"),
            "databricks_token_set": bool(cls.DATABRICKS_TOKEN),
            "catalog_schema_set": bool(cls.CATALOG_NAME and cls.SCHEMA_NAME),
        }
        
        if cls.ENABLE_NOTIFICATIONS and "slack" in cls.NOTIFICATION_HANDLERS:
            validation["slack_webhook_set"] = bool(cls.SLACK_WEBHOOK_URL)
        
        if cls.ENABLE_NOTIFICATIONS and "email" in cls.NOTIFICATION_HANDLERS:
            validation["email_config_set"] = all([
                cls.SMTP_SERVER, cls.SMTP_USERNAME, 
                cls.SMTP_PASSWORD, cls.EMAIL_FROM
            ])
        
        return validation


class UIConfig:
    """UI-specific configuration"""
    
    # Color scheme
    COLORS = {
        "primary": "#FF6B35",
        "secondary": "#004E89", 
        "success": "#00C851",
        "warning": "#FF8800",
        "danger": "#FF4444",
        "info": "#33B5E5",
        "light": "#F8F9FA",
        "dark": "#343A40"
    }
    
    # Status colors
    STATUS_COLORS = {
        "pending": "#FF8800",
        "running": "#33B5E5", 
        "completed": "#00C851",
        "approved": "#00C851",
        "rejected": "#FF4444",
        "failed": "#FF4444",
        "cancelled": "#6C757D"
    }
    
    # Icons
    ICONS = {
        "job": "🎯",
        "approval": "✅",
        "rejection": "❌", 
        "pending": "⏳",
        "running": "🔄",
        "completed": "🎉",
        "failed": "💥",
        "user": "👤",
        "admin": "🔧",
        "notification": "📧"
    }
    
    # Page settings
    ITEMS_PER_PAGE = 10
    MAX_DISPLAY_CHARS = 100
    
    @classmethod
    def get_status_display(cls, status: str) -> Dict[str, str]:
        """Get display properties for a status"""
        return {
            "color": cls.STATUS_COLORS.get(status.lower(), "#6C757D"),
            "icon": cls.ICONS.get(status.lower(), "❓")
        }


class SecurityConfig:
    """Security-related configuration"""
    
    # Session settings
    SESSION_TIMEOUT_MINUTES = int(os.getenv("SESSION_TIMEOUT_MINUTES", "60"))
    
    # Role definitions
    ROLES = {
        "requester": {
            "can_create_requests": True,
            "can_approve_requests": False,
            "can_view_all_requests": False,
            "can_admin": False
        },
        "approver": {
            "can_create_requests": True,
            "can_approve_requests": True,
            "can_view_all_requests": True,
            "can_admin": False
        },
        "admin": {
            "can_create_requests": True,
            "can_approve_requests": True,
            "can_view_all_requests": True,
            "can_admin": True
        }
    }
    
    @classmethod
    def get_user_permissions(cls, role: str) -> Dict[str, bool]:
        """Get permissions for a user role"""
        return cls.ROLES.get(role.lower(), cls.ROLES["requester"])
    
    @classmethod
    def can_user_perform_action(cls, user_role: str, action: str) -> bool:
        """Check if user can perform a specific action"""
        permissions = cls.get_user_permissions(user_role)
        return permissions.get(action, False)
