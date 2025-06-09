import os
from azure.storage.blob.aio import BlobServiceClient
from fastapi import UploadFile
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class AzureStorageManager:
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "documents")
        if not self.connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is required")

    async def upload_file(self, file: UploadFile, filename: str, session_id: str) -> str:
        """
        Upload a file to Azure Blob Storage
        
        Args:
            file (UploadFile): The file to upload
            filename (str): Name to use for the blob
            
        Returns:
            str: URL of the uploaded blob
        """
        # Create blob service client
        async with BlobServiceClient.from_connection_string(self.connection_string) as blob_service_client:
            # Get container client
            container_client = blob_service_client.get_container_client(self.container_name)
            
            try:
                # Create container if it doesn't exist
                await container_client.create_container()
                logger.info(f"Container {self.container_name} created or already exists")
            except Exception as e:
                logger.warning(f"Container creation warning: {str(e)}")
            
            # Get blob client
            blob_client = container_client.get_blob_client(session_id + "/" + filename)
            
            try:
                # Read file content
                file_content = await file.read()
                
                # Upload the file
                await blob_client.upload_blob(file_content, overwrite=True)
                logger.info(f"Successfully uploaded file: {filename}")
                
                # Get the blob URL
                blob_url = blob_client.url
                
                return blob_url
            except Exception as e:
                logger.error(f"Error uploading file {filename}: {str(e)}")
                raise 