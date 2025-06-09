from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from typing import List
import asyncio
import uuid
from datetime import datetime
import logging
import tempfile
import os
import aiofiles
import mimetypes
from pydantic import BaseModel
from sqlalchemy.orm import Session
from azure_storage import AzureStorageManager
from database import get_db, engine
from models import Document, Base

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Document Processing API")

# Initialize Azure Storage
azure_storage = AzureStorageManager()

def get_generic_doc_type(mime_type: str, filename: str = None) -> str:
    """
    Map MIME types to generic document types.
    
    Args:
        mime_type (str): MIME type of the document
        filename (str, optional): Original filename to check extension if MIME type is generic
        
    Returns:
        str: Generic document type (PDF, WORD, IMAGE, EMAIL, OTHER)
    """
    # Convert mime_type to lowercase for consistent comparison
    mime_type = mime_type.lower() if mime_type else ''
    
    # Handle email type
    if mime_type == 'email' or (filename and filename.startswith('Email_')):
        return 'EMAIL'
    
    # Handle PDF
    if 'pdf' in mime_type:
        return 'PDF'
    
    # Handle Word documents
    if any(word_type in mime_type for word_type in ['word', 'wordprocessing', 'msword', 'openxmlformats-officedocument.wordprocessingml']):
        return 'WORD'
    
    # Handle Images
    if any(img_type in mime_type for img_type in ['image/', 'jpeg', 'jpg', 'png', 'gif', 'bmp', 'tiff']):
        return 'IMAGE'
    
    # Handle Text files
    if 'text/' in mime_type or mime_type == 'application/txt':
        return 'TEXT'
    
    # Check file extension if MIME type is generic
    if filename and (mime_type == 'application/octet-stream' or not mime_type):
        ext = os.path.splitext(filename.lower())[1]
        extension_mapping = {
            '.pdf': 'PDF',
            '.doc': 'WORD',
            '.docx': 'WORD',
            '.txt': 'TEXT',
            '.jpg': 'IMAGE',
            '.jpeg': 'IMAGE',
            '.png': 'IMAGE',
            '.gif': 'IMAGE',
            '.jfif': 'IMAGE',
            '.eml': 'EMAIL',
        }
        return extension_mapping.get(ext, 'OTHER')
    
    return 'OTHER'

try:
    # Create database tables
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully")
except Exception as e:
    logger.error(f"Error creating database tables: {str(e)}")
    raise


class FetchEmailsBody(BaseModel):
    email_id: str
    email_type: str
    
    
@app.post("/fetch-emails/")
async def fetch_emails(
    body: FetchEmailsBody
):
    """
    Endpoint to fetch emails from Microsoft 365
    """
    return {
        "status": "success", 
        "message": "Email fetched successfully",
        "result": [
            {
                "sender_email_id": body.email_id,
                "email_subject": "Test Email",
                "email_body": "This is a test email",
            },
            {
                "sender_email_id": body.email_id,
                "email_subject": "Test Email 2",
                "email_body": "This is a test email 2",
            },
        ]
        }


class Email(BaseModel):
    sender_email_id: str
    email_subject: str
    email_body: str

class EmailBody(BaseModel):
    emails: List[Email]
    session_id: str

@app.post("/upload-email/")
async def upload_email(
    body: EmailBody,
    db: Session = Depends(get_db)
):
    """
    Endpoint to upload an email:
    1. Create a file with name Email_<doc_id>
    2. Write email content to the file
    3. Upload file to Azure Storage
    4. Save metadata in database
    """
    try:
        result = []
        session_id = body.session_id
        for email in body.emails:
            # Generate document ID,
            subject = email.email_subject.replace(" ", "_")
            file_name = f"{subject}.txt"
            
            # Create a temporary file with the specific name
            temp_dir = tempfile.gettempdir()
            file_path = os.path.join(temp_dir, file_name)
            
            # Write email content to file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(email.email_body)
            
            # Create UploadFile object for Azure Storage
            async with aiofiles.open(file_path, 'rb') as f:
                content = await f.read()
                upload_file = UploadFile(
                    filename=file_name,
                    file=tempfile.SpooledTemporaryFile()
                )
                await upload_file.write(content)
                await upload_file.seek(0)
                
                # Upload to Azure Storage
                azure_blob_url = await azure_storage.upload_file(upload_file, file_name, session_id)
                logger.info(f"Email file uploaded to Azure Storage: {file_name}")
            
            # Clean up temporary file
            os.remove(file_path)
            
            result.append({
                "status": "success",
                # "message": f"Email document created and saved to database: {doc_id}",
                # "doc_id": doc_id,
                "file_name": file_name,
                # "doc_type": document.doc_type,
                "azure_blob_url": azure_blob_url
            })
        return {
            "message": f"Processed {len(body.emails)} emails",
            "results": result
        }
    except Exception as e:
        logger.error(f"Error creating email document: {str(e)}")
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=500, detail=str(e))
    


@app.post("/upload-documents/")
async def upload_documents(
    session_id: str,
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db)
):
    """
    Endpoint to upload multiple documents:
    1. Store files in Azure Blob Storage
    2. Save metadata in PostgreSQL database
    
    Query Parameters:
        session_id: str - The session identifier for grouping uploaded files
    """
    results = []
    
    for file in files:
        try:
            logger.info(f"Processing file: {file.filename}")
            
            # Upload to Azure Storage
            azure_blob_url = await azure_storage.upload_file(file, file.filename, session_id)
            logger.info(f"File uploaded to Azure Storage: {file.filename}")
            results.append({
                "file_name": file.filename,
                "status": "success",
                "azure_blob_url": azure_blob_url,
            })        
        except Exception as e:
            logger.error(f"Error processing file {file.filename}: {str(e)}")
            results.append({
                "doc_name": file.filename,
                "status": "error",
                "error": str(e)
            })
    
    return {
        "message": f"Processed {len(files)} documents",
        "results": results
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
