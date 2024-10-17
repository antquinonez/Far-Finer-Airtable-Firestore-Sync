import os
from google.cloud import secretmanager
from typing import Optional, List

class Secrets:

    @classmethod
    def _get_project_id(cls) -> str:
        """Retrieve the Google Cloud project ID."""
        print("Attempting to retrieve Google Cloud project ID")
        try:
            import google.auth
            _, project_id = google.auth.default()
            if project_id:
                print(f"Project ID retrieved: {project_id}")
                return project_id
            print("Failed to determine Google Cloud project ID")
            raise ValueError("Unable to determine Google Cloud project ID")
        except Exception as e:
            print(f"Error determining Google Cloud project ID: {str(e)}")
            raise ValueError(f"Error determining Google Cloud project ID: {str(e)}")

    @classmethod
    def _get_api_key_from_secret_manager(cls, key_name: str) -> Optional[str]:
        """Attempt to retrieve the API key from Google Cloud Secret Manager."""
        print("Attempting to retrieve API key from Google Cloud Secret Manager")
        try:            
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{cls._get_project_id()}/secrets/{key_name}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            print("API key successfully retrieved from Secret Manager")
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"Failed to retrieve API key from Google Cloud Secret Manager: {str(e)}")
            return None

    @classmethod
    def get_api_key(cls, key_name: str) -> str:
        """Retrieve the API key from Google Cloud Secret Manager or environment variables."""
        print("Attempting to retrieve API key")
        api_key = cls._get_api_key_from_secret_manager(key_name)
        if api_key:
            print("API key retrieved from Secret Manager")
            return api_key
        
        api_key = os.getenv(key_name)
        if api_key:
            print("API key retrieved from environment variables")
            return api_key
        
        print("Failed to retrieve API key")
        raise ValueError(f"{key_name} not found in environment variables and Secret Manager access failed")