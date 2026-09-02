from cryptography.fernet import Fernet
import logging

class PasswordManager:
    def __init__(self, key=None):
        if key:
            self.fernet = Fernet(key)
        else:
            # Generate a key for encryption (You should keep this key secret)
            self.key = Fernet.generate_key()
            self.fernet = Fernet(self.key)

        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def encrypt_password(self, key, password: str) -> str:
        """Encrypt a password and return the encrypted version."""
        fernet = Fernet(key)
        encrypted_password = fernet.encrypt(password.encode())
        self.logger.info(f"Password encrypted.")
        return encrypted_password.decode()

    def decrypt_password(self, key, encrypted_password: str) -> str:
        """Decrypt an encrypted password and return the original password."""
        fernet = Fernet(key)
        decrypted_password = fernet.decrypt(encrypted_password.encode()).decode()
        self.logger.info(f"Password decrypted.")
        return decrypted_password


if __name__ == "__main__":
    # Key for encryption and decryption (keep this key secure)
    encryption_key = Fernet.generate_key()
    print(encryption_key)

    password_manager = PasswordManager(key=encryption_key)

    # Password to be encrypted
    password = "MohHll_10123"


    encrypted_password = password_manager.encrypt_password(encryption_key, password)

    password_manager.logger.info(f"Original Password: {password}")
    password_manager.logger.info(f"Encrypted Password: {encrypted_password}")
    encryption_key = encryption_key.decode()
    password_manager.logger.info(f"Encrypted Key: {encryption_key}")
    decrypted_password = password_manager.decrypt_password(encryption_key, encrypted_password)
    password_manager.logger.info(f"Decrypted Password: {decrypted_password}")
