from sqlalchemy import Column, String, DateTime, func
from database import Base

class Document(Base):
    __tablename__ = "documents"
    __table_args__ = {"schema": "requirementsbits"}

    doc_id = Column(String, primary_key=True, index=True)
    doc_name = Column(String, nullable=False)
    doc_type = Column(String)
    status = Column(String, default="QUEUED")
    azure_blob_url = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now()) 
    session_id = Column(String, nullable=False)
    extracted_text = Column(String)
    entity_list = Column(String)
