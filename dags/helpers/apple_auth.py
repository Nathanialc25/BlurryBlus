import jwt
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class AppleAuthManager:
    """
    A class to manage Apple Music API JWT authentication.
    Handles token generation, validation, and storage.
    """
    
    def __init__(self, team_id: str, key_id: str, private_key_path: str, jwt_store_path: str):
        """
        Initialize the Apple Auth Manager.
        
        Args:
            team_id: Apple Developer Team ID
            key_id: Apple Music Key ID
            private_key_path: Path to the .p8 private key file
            jwt_store_path: Path where JWT token will be stored/read
        """
        self.team_id = team_id
        self.key_id = key_id
        self.private_key_path = private_key_path
        self.jwt_store_path = jwt_store_path
    
    def _load_private_key(self) -> str:
        """
        Load and validate the private key from file.
        
        Returns:
            The private key content as a string
            
        Raises:
            FileNotFoundError: If private key file doesn't exist
            ValueError: If private key format is invalid
        """
        if not os.path.exists(self.private_key_path):
            raise FileNotFoundError(f"Private key not found at {self.private_key_path}")
        
        with open(self.private_key_path, 'r') as f:
            key_content = f.read().strip()
        
        # Validate key format
        if not key_content.startswith("-----BEGIN PRIVATE KEY-----"):
            raise ValueError("Private key missing BEGIN header")
        if not key_content.endswith("-----END PRIVATE KEY-----"):
            raise ValueError("Private key missing END footer")
        
        return key_content
    
    def check_jwt_validity(self) -> bool:
        """
        Check if existing JWT is still valid.
        
        Returns:
            True if valid token exists, False otherwise
        """
        try:
            if not os.path.exists(self.jwt_store_path):
                logger.debug("JWT store file does not exist")
                return False
            
            with open(self.jwt_store_path, "r") as f:
                content = f.read().strip().split('\n')
                
            if len(content) < 2:
                logger.debug("JWT store file malformed - insufficient lines")
                return False
                
            token, expiration_str = content[0], content[1]
            expiration = datetime.fromisoformat(expiration_str)
            
            if expiration > datetime.utcnow():
                logger.info("Valid JWT token found (expires: %s)", expiration)
                return True
            else:
                logger.info("JWT token expired on %s", expiration)
                return False
                
        except Exception as e:
            logger.error("Error checking JWT validity: %s", str(e))
            return False
    
    def generate_jwt(self) -> Tuple[str, datetime]:
        """
        Generate a new JWT token.
        
        Returns:
            Tuple of (token_string, expiration_datetime)
            
        Raises:
            Exception: If JWT generation fails
        """
        try:
            issued_at = datetime.utcnow()
            expiration = issued_at + timedelta(days=179)  # 180 days max, give 1 day buffer
            
            headers = {
                "alg": "ES256",
                "kid": self.key_id,
                "typ": "JWT"
            }
            
            payload = {
                "iss": self.team_id,
                "iat": int(issued_at.timestamp()),
                "exp": int(expiration.timestamp()),
            }
            
            private_key = self._load_private_key()
            token = jwt.encode(payload, private_key, algorithm="ES256", headers=headers)
            
            logger.info("Successfully generated new JWT token")
            return token, expiration
            
        except Exception as e:
            logger.error("Failed to generate JWT: %s", str(e))
            raise
    
    def ensure_valid_jwt(self) -> str:
        """
        Ensure we have a valid JWT, generating one if needed.
        
        Returns:
            Valid JWT token string
            
        Raises:
            Exception: If token generation fails
        """
        if self.check_jwt_validity():
            with open(self.jwt_store_path, "r") as f:
                return f.read().split('\n')[0].strip()
        
        token, expiration = self.generate_jwt()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.jwt_store_path), exist_ok=True)
        
        with open(self.jwt_store_path, "w") as f:
            f.write(f"{token}\n{expiration.isoformat()}\n")
        
        logger.info("Generated new JWT valid until %s", expiration.isoformat())
        return token
    
    def get_current_token(self) -> Optional[str]:
        """
        Get the current token if valid, None otherwise.
        
        Returns:
            Current valid token or None
        """
        if self.check_jwt_validity():
            with open(self.jwt_store_path, "r") as f:
                return f.read().split('\n')[0].strip()
        return None