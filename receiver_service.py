import asyncio
from azure.storage.blob import BlobServiceClient
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from models import Document, Base
import os
import uuid
import logging
import mimetypes
from typing import List, Dict
from datetime import datetime
from database import get_db

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReceiverService:
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
    
    def get_generic_doc_type(self, blob_name: str) -> str:
        """
        Determine document type based on file extension
        """
        _, ext = os.path.splitext(blob_name.lower())
        extension_mapping = {
            '.pdf': 'PDF',
            '.doc': 'WORD',
            '.docx': 'WORD',
            '.txt': 'EMAIL',
            '.jpg': 'IMAGE',
            '.jpeg': 'IMAGE',
            '.png': 'IMAGE',
            '.gif': 'IMAGE',
            '.jfif': 'IMAGE',
            '.eml': 'EMAIL',
        }
        return extension_mapping.get(ext, 'OTHER')

    async def process_session_documents(self, session_id: str) -> Dict:
        """
        Fetch all documents from a session folder in Azure Storage and save to database
        
        Args:
            session_id: The session identifier
            
        Returns:
            Dict containing processing results
        """
        db_generator = get_db()
        db = next(db_generator)
        try:
            logger.info(f"Processing documents for session: {session_id}")
            results = []
            
            # List all blobs in the session folder
            blob_list = self.container_client.list_blobs(name_starts_with=f"{session_id}/")
            
            for blob in blob_list:
                try:
                    # Get blob details
                    blob_name = blob.name
                    file_name = os.path.basename(blob_name)
                    doc_type = self.get_generic_doc_type(file_name)
                    
                    
                    # Check if document already exists
                    existing_doc = db.query(Document).filter(
                        Document.doc_name == file_name,
                        Document.session_id == session_id,
                        Document.doc_type == doc_type
                    ).first()
                    
                    if existing_doc:
                        logger.info(f"Document already exists: {file_name}")
                        results.append({
                            "doc_id": existing_doc.doc_id,
                            "doc_name": existing_doc.doc_name,
                            "doc_type": existing_doc.doc_type,
                            "status": "exists",
                            "azure_blob_url": existing_doc.azure_blob_url
                        })
                        continue
                    
                    # Get blob URL
                    blob_client = self.container_client.get_blob_client(blob_name)
                    azure_blob_url = blob_client.url
                    
                    # Create new document record
                    doc_id = str(uuid.uuid4())
                    
                    document = Document(
                        doc_id=doc_id,
                        doc_name=file_name,
                        doc_type=doc_type,
                        status="RECEIVED",
                        azure_blob_url=azure_blob_url,
                        created_at=datetime.utcnow(),
                        session_id=session_id,
                    )
                    
                    # Save to database
                    db.add(document)
                    db.commit()
                    db.refresh(document)
                    
                    logger.info(f"Document processed successfully: {file_name}")
                    results.append({
                        "doc_id": doc_id,
                        "doc_name": file_name,
                        "doc_type": doc_type,
                        "status": "success",
                        "azure_blob_url": azure_blob_url
                    })
                    
                except Exception as doc_error:
                    logger.error(f"Error processing document {blob_name}: {str(doc_error)}")
                    db.rollback()
                    results.append({
                        "doc_name": blob_name,
                        "status": "error",
                        "error": str(doc_error)
                    })
            
            return {
                "message": f"Processed {len(results)} documents for session {session_id}",
                "results": results
            }
            
        except Exception as e:
            logger.error(f"Error processing session {session_id}: {str(e)}")
            raise Exception(f"Failed to process session: {str(e)}")
        finally:
            db.close()

async def main():
    try:    
        receiver = ReceiverService()
        while True:
            try:
                session_id = input("Enter session ID (or 'exit' to quit): ")
                if session_id.lower() == 'exit':
                    break
                    
                results = await receiver.process_session_documents(session_id)
                print("\nProcessing Results:")
                print(f"Message: {results['message']}")
                print("\nDetailed Results:")
                for doc in results['results']:
                    print(f"\nDocument: {doc['doc_name']}")
                    print(f"Status: {doc['status']}")
                    if doc['status'] == 'error':
                        print(f"Error: {doc['error']}")
                    else:
                        print(f"Type: {doc['doc_type']}")
                        print(f"URL: {doc['azure_blob_url']}")
                print("\n" + "="*50 + "\n")
                
            except Exception as e:
                print(f"\nError: {str(e)}")
                print("\n" + "="*50 + "\n")
                
    except KeyboardInterrupt:
        print("\nExiting the program...")
    except Exception as e:
        print(f"\nFatal error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
