import asyncio
import os
from azure.storage.blob import BlobServiceClient
from sqlalchemy.orm import Session
from models import Document, Base
import os
from database import get_db
import logging
import PyPDF2
from PIL import Image
import pytesseract
from docx import Document as WordDocument
import tempfile

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Tesseract path for Windows
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

class ExtractorService:
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    async def process_session_documents(self, session_id: str):
        """
        Download all documents from a session folder in Azure Storage and extract text.
        The documents can be of different types, including PDF, Word, and image files.
        check the doc_type to determine the appropriate extraction method.
        For images, OCR will be used using Tesseract OCR.
        For PDF and Word files, text extraction will be performed using PyPDF2 and python-docx libraries.
        for text files, the content will be read as is.
        The extracted text will be saved in the database.
        Args:
            session_id: The session identifier
            
        Returns:
            Dict containing processing results
        """
        db_generator = get_db()
        db = next(db_generator)
        try:
            logger.info(f"Processing documents for session: {session_id}")
            doc_list = db.query(Document).filter(
                Document.session_id == session_id
            ).all()
            
            for doc in doc_list:
                logger.info(f"Processing document: {doc.doc_name}") 
                logger.info(f"Document URL: {doc.azure_blob_url}")
                # Download the document from Azure Storage
                blob_client = self.container_client.get_blob_client(session_id + "/" + doc.doc_name)
                local_file_path = os.path.join("temp", doc.doc_name)
                os.makedirs("temp", exist_ok=True)
                with open(local_file_path, "wb") as file:
                    file.write(blob_client.download_blob().readall())
                
                # Extract text based on document type
                if doc.doc_type == "PDF":
                    text = self.extract_text_from_pdf(local_file_path)
                elif doc.doc_type == "WORD":
                    text = self.extract_text_from_word(local_file_path)
                elif doc.doc_type == "IMAGE":
                    text = self.extract_text_from_image(local_file_path)
                elif doc.doc_type == "EMAIL":
                    with open(local_file_path, "r") as file:
                        text = file.read()
                else:
                    text = ""
                # refine the text using LLM
                if text:
                    refined_text = self.refine_text_using_llm(text)
                    entity_list = self.extract_entities_using_llm(refined_text)
                else:
                    refined_text = ""
                    entity_list = ""
                
                # save the extracted text to the database
                doc.extracted_text = refined_text
                doc.entity_list = entity_list
                db.commit()
                db.refresh(doc)
                logger.info(f"Document {doc.doc_name} processed successfully")
            
            logger.info(f"All documents for session {session_id} processed successfully")
            return {"status": "success", "message": f"All documents for session {session_id} processed successfully"}
        
        except Exception as e:
            logger.error(f"Error processing session {session_id}: {str(e)}")
            raise Exception(f"Failed to process session: {str(e)}")
        finally:
            db.close()
    
    def extract_text_from_pdf(self, file_path: str) -> str:
        """
        Extract text from a PDF file using PyPDF2.
        Handles corrupted PDFs and adds robust error handling.
        """
        try:
            text = ""
            # Try PyPDF2 with strict=False for corrupted PDFs
            with open(file_path, "rb") as file:
                try:
                    pdf_reader = PyPDF2.PdfReader(file, strict=False)
                    for page_num in range(len(pdf_reader.pages)):
                        try:
                            page = pdf_reader.pages[page_num]
                            page_text = page.extract_text()
                            if page_text.strip():
                                text += page_text + "\n"
                            else:
                                logger.warning(f"No text found in page {page_num + 1}")
                        except Exception as e:
                            logger.error(f"Error processing page {page_num + 1}: {str(e)}")
                            continue
                except Exception as e:
                    logger.error(f"Error reading PDF: {str(e)}")
                    return ""
            return text.strip()
        except Exception as e:
            logger.error(f"Error extracting text from PDF: {str(e)}")
            return ""
    
    def extract_text_from_word(self, file_path: str) -> str:
        """
        Extract text from a Word file using python-docx
        """
        text = ""
        doc = WordDocument(file_path)
        for paragraph in doc.paragraphs:
            text += paragraph.text
        return text
    
    def extract_text_from_image(self, file_path: str) -> str:
        """
        Extract text from an image using Tesseract OCR
        """
        try:
            text = ""
            image = Image.open(file_path)
            text = pytesseract.image_to_string(image)
            return text
        except Exception as e:
            logger.error(f"Error in OCR processing: {str(e)}")
            return ""
    
    def refine_text_using_llm(self, text: str) -> str:
        """
        Refine the extracted text using a LLM
        """
        return text
    
    def extract_entities_using_llm(self, text: str) -> str:
        """
        Extract entities from the refined text using a LLM.
        Perform Named Entity Recognition (NER) on the refined text.
        """
        llm_response = [
            {
                "entity_name": "Person",
                "entity_value": "John Doe"
            },
            {
                "entity_name": "Organization",
                "entity_value": "Acme Inc."
            }
        ]
        # TODO: call the LLM to extract entities
        entity_list = []
        for entity in llm_response:
            entity_list.append(f"{entity['entity_name']}: {entity['entity_value']}")
        return "\n".join(entity_list)
    
async def get_input():
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, input, "Enter session ID (or 'exit' to quit): ")

async def main():
    receiver = ExtractorService()
    try:
        while True:
            try:
                session_id = await get_input()
                if session_id.lower() == 'exit':
                    break
                    
                results = await receiver.process_session_documents(session_id)
                logger.info("\nProcessing Results:")
                logger.info(results)
            except Exception as e:
                logger.error(f"\nError: {str(e)}")
                logger.info("\n" + "="*50 + "\n")
    finally:
        # Clean up temp directory if it exists
        if os.path.exists("temp"):
            try:
                for file in os.listdir("temp"):
                    os.remove(os.path.join("temp", file))
                os.rmdir("temp")
            except Exception as e:
                logger.error(f"Error cleaning up temp directory: {str(e)}")
        logger.info("Cleanup complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nReceived interrupt signal. Exiting gracefully...")
    except Exception as e:
        logger.error(f"\nFatal error: {str(e)}")
    finally:
        logger.info("Program terminated.")
