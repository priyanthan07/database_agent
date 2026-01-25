import os
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseModel):
    """Application settings from environment variables"""
    
    # KG Storage Database (kg_storage_db)
    KG_USER: str = os.getenv("KG_USER")
    KG_PASSWORD: str = os.getenv("KG_PASSWORD")
    KG_HOST: str = os.getenv("KG_HOST")
    KG_PORT: int = int(os.getenv("KG_PORT"))
    KG_DATABASE: str = os.getenv("KG_DATABASE")
    
    # OpenAI
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY")
    
    # Langfuse (optional - auto-enabled if keys present)
    LANGFUSE_PUBLIC_KEY: str = os.getenv("LANGFUSE_PUBLIC_KEY")
    LANGFUSE_SECRET_KEY: str = os.getenv("LANGFUSE_SECRET_KEY")
    LANGFUSE_HOST: str = os.getenv("LANGFUSE_HOST")
    
    # Chroma
    CHROMA_PERSIST_DIR: str = os.getenv("CHROMA_PERSIST_DIR")
    
    @property
    def enable_langfuse(self) -> bool:
        """Check if Langfuse monitoring should be enabled"""
        return bool(self.LANGFUSE_PUBLIC_KEY and self.LANGFUSE_SECRET_KEY)
    
    class Config:
        env_file = ".env"