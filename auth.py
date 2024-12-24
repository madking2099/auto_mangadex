import json
import requests
import logging
import os
import getpass
import time
from typing import Dict, Optional
from requests.exceptions import RequestException
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Setup logging with configuration
logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG' if os.environ.get('DEBUG', 'False').lower() == 'true' else 'INFO',
            'formatter': 'detailed'
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'auth.log',
            'level': 'INFO',
            'formatter': 'detailed'
        }
    },
    'loggers': {
        __name__: {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
            'propagate': False,
        },
    }
})

logger = logging.getLogger(__name__)

# Check for .env file and load or generate ENCRYPTION_KEY
env_file = ".env"

if os.path.exists(env_file):
    logger.info(f"Loading environment variables from {env_file}")
    load_dotenv(env_file)
    if not os.environ.get('ENCRYPTION_KEY'):
        logger.info(f"{env_file} not found, generating new ENCRYPTION_KEY")
        key = Fernet.generate_key().decode()  # Decode to string for .env file
        with open(env_file, 'a') as f:
            f.write(f"ENCRYPTION_KEY={key}\n")
            load_dotenv(env_file)
    key = os.environ.get('ENCRYPTION_KEY')
    if not key:
        raise ValueError(f"ENCRYPTION_KEY not found in {env_file}")
else:
    logger.info(f"{env_file} not found, generating new ENCRYPTION_KEY")
    key = Fernet.generate_key().decode()  # Decode to string for .env file
    with open(env_file, 'w') as f:
        f.write(f"ENCRYPTION_KEY={key}\n")
        f.write(f"MANGADEX_LOGIN_ENDPOINT=https://api.mangadex.org/auth/login\n")
        f.write(f"MANGADEX_LOGOUT_ENDPOINT=https://api.mangadex.org/auth/logout\n")
    load_dotenv(env_file)  # Load the newly created .env file
    logger.info(f"New {env_file} created with ENCRYPTION_KEY and MangaDex endpoints")

# If .env exists but doesn't have the MangaDex endpoints, add them
if not os.environ.get('MANGADEX_LOGIN_ENDPOINT'):
    logger.info(f"Adding MANGADEX_LOGIN_ENDPOINT to {env_file}")
    with open(env_file, 'a') as f:
        f.write(f"MANGADEX_LOGIN_ENDPOINT=https://api.mangadex.org/auth/login\n")

if not os.environ.get('MANGADEX_LOGOUT_ENDPOINT'):
    logger.info(f"Adding MANGADEX_LOGOUT_ENDPOINT to {env_file}")
    with open(env_file, 'a') as f:
        f.write(f"MANGADEX_LOGOUT_ENDPOINT=https://api.mangadex.org/auth/logout\n")

# Reload .env to ensure new additions are in environment
load_dotenv(env_file)

cipher_suite = Fernet(key.encode())  # Encode back to bytes for Fernet

# Use environment variables for API endpoints
MANGADEX_LOGIN_ENDPOINT = os.environ.get('MANGADEX_LOGIN_ENDPOINT', 'https://api.mangadex.org/auth/login')
MANGADEX_LOGOUT_ENDPOINT = os.environ.get('MANGADEX_LOGOUT_ENDPOINT', 'https://api.mangadex.org/auth/logout')


class AuthenticationError(Exception):
    """Custom exception for authentication related errors."""
    pass


class AuthManager:
    """
    Manages authentication for interacting with MangaDex API and local application access.

    This class handles:
    - User authentication via username/password
    - Session token management for API calls
    - Storage and retrieval of encrypted authentication credentials
    - Logout functionality to remove session tokens and clear credentials
    - Token expiry management

    Attributes:
        _session_token (str): The session token used for API access.
        _user_credentials (Dict[str, bytes]): Stores encrypted user credentials for local authentication.
        _token_expiry (int): Unix timestamp when the session token expires, if known.
    """

    def __init__(self):
        """Initialize the AuthManager with empty session and credentials."""
        self._session_token: Optional[str] = None
        self._user_credentials: Dict[str, bytes] = {}
        self._token_expiry: Optional[int] = None

    def authenticate_with_credentials(self, username: str, password: str) -> str:
        """
        Authenticate using username and password to obtain a session token from MangaDex API.

        Args:
            username (str): User's username.
            password (str): User's password.

        Returns:
            str: Session token for API access.

        Raises:
            AuthenticationError: If authentication fails.
        """
        try:
            response = requests.post(
                MANGADEX_LOGIN_ENDPOINT,
                json={"username": username, "password": password},
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            )
            response.raise_for_status()
            data = response.json()
            if 'token' in data:
                self._session_token = data['token']
                # Assume token expiry is provided in seconds; adjust based on actual API response
                self._token_expiry = int(time.time()) + int(
                    data.get('expires_in', 3600)) if 'expires_in' in data else None
                logger.info(f"Successfully authenticated user {username}")
                return self._session_token
            else:
                raise AuthenticationError("Unexpected response from MangaDex API: No token provided")
        except RequestException as e:
            logger.error(f"Error authenticating with MangaDex API: {e}")
            raise AuthenticationError(f"Failed to authenticate with MangaDex API: {e}")

    def logout(self):
        """
        Log out from MangaDex by sending a POST request to the logout endpoint with the session token.

        This method attempts to send a logout request to MangaDex and then clears all local
        authentication data.

        Raises:
            AuthenticationError: If there's an issue with the logout process.
        """
        try:
            if self._session_token:
                response = requests.post(
                    MANGADEX_LOGOUT_ENDPOINT,
                    headers={
                        "Authorization": f"Bearer {self._session_token}"
                    }
                )
                response.raise_for_status()  # Ensure we got a successful response
                logger.info("Successfully logged out from MangaDex")
            else:
                logger.warning("No session token to logout with")

            # Clear local authentication data
            self._session_token = None
            self._user_credentials.clear()
            self._token_expiry = None
            logger.info("Local authentication data cleared")

        except RequestException as e:
            logger.error(f"Error during logout: {e}")
            raise AuthenticationError(f"Failed to logout from MangaDex API: {e}")

    def is_token_expired(self) -> bool:
        """
        Check if the current session token has expired.

        Returns:
            bool: True if the token is expired, False otherwise or if no expiry info is available.
        """
        return self._token_expiry is not None and time.time() > self._token_expiry

    def store_user_credentials(self, username: str, password: str):
        """
        Store encrypted user credentials for application authentication.

        Args:
            username (str): User's username.
            password (str): User's password.
        """
        if not self.validate_username(username) or not self.validate_password(password):
            raise ValueError("Invalid username or password format.")

        encrypted_password = cipher_suite.encrypt(password.encode())
        self._user_credentials[username] = encrypted_password
        logger.info(f"Stored credentials for user {username}")

    def get_decrypted_password(self, username: str) -> Optional[str]:
        """
        Retrieve and decrypt the password for a given username.

        Args:
            username (str): User's username.

        Returns:
            Optional[str]: The decrypted password or None if not found.
        """
        if username in self._user_credentials:
            return cipher_suite.decrypt(self._user_credentials[username]).decode()
        return None

    def get_session_token(self) -> Optional[str]:
        """
        Retrieve the current session token if it hasn't expired.

        Returns:
            Optional[str]: The session token or None if not authenticated or token expired.
        """
        if self.is_token_expired():
            logger.warning("Session token has expired.")
            return None
        return self._session_token

    def save_to_json(self, file_path: str):
        """
        Save user credentials to a JSON file for persistence.

        Args:
            file_path (str): Path to save the JSON file.

        Note:
            This method doesn't save the session token due to security concerns.
        """
        with open(file_path, 'w') as f:
            # We'll save encrypted credentials
            json.dump({k: v.decode() for k, v in self._user_credentials.items()}, f)
        logger.info(f"Saved credentials to {file_path}")

    def load_from_json(self, file_path: str):
        """
        Load user credentials from a JSON file.

        Args:
            file_path (str): Path to the JSON file.

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the JSON is malformed.
        """
        try:
            with open(file_path, 'r') as f:
                loaded_credentials = json.load(f)
                self._user_credentials = {k: cipher_suite.encrypt(v.encode()) for k, v in loaded_credentials.items()}
            logger.info(f"Loaded credentials from {file_path}")
        except FileNotFoundError:
            logger.error(f"Credentials file not found at {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from file {file_path}: {e}")
            raise

    @staticmethod
    def validate_username(username: str) -> bool:
        """
        Validate username format.

        Args:
            username (str): The username to validate.

        Returns:
            bool: True if username is valid, False otherwise.
        """
        # Example: Username should be 3 to 20 characters long, alphanumeric
        return 3 <= len(username) <= 20 and username.isalnum()

    @staticmethod
    def validate_password(password: str) -> bool:
        """
        Validate password format.

        Args:
            password (str): The password to validate.

        Returns:
            bool: True if password is valid, False otherwise.
        """
        # Example: Password should be at least 8 characters long, contain at least one digit and one letter
        return len(password) >= 8 and any(char.isdigit() for char in password) and any(
            char.isalpha() for char in password)


def validate_input(input_str: str) -> bool:
    """
    Validate that the input is a non-empty string.

    Args:
        input_str (str): Input string to validate.

    Returns:
        bool: True if input is valid, False otherwise.
    """
    return isinstance(input_str, str) and input_str.strip() != ""


if __name__ == "__main__":
    auth_manager = AuthManager()

    try:
        while True:
            command = input("Enter 'login', 'logout', 'save', 'load', or 'quit': ").lower().strip()

            if command == 'login':
                uid1 = input("Please Enter a UID: \n").strip()
                while not validate_input(uid1) or not AuthManager.validate_username(uid1):
                    uid1 = input("Invalid UID. Please enter again: \n").strip()

                passwd1 = getpass.getpass("Please Enter a Password: ")
                while not validate_input(passwd1) or not AuthManager.validate_password(passwd1):
                    passwd1 = getpass.getpass("Invalid Password. Please enter again: ")

                save_creds = input("Do you want to save these credentials? (y/n): ").lower() == 'y'
                if save_creds:
                    auth_manager.store_user_credentials(username=uid1, password=passwd1)
                session_token = auth_manager.authenticate_with_credentials(username=uid1, password=passwd1)
                print(f"Session Token: {session_token}")
                if auth_manager.is_token_expired():
                    print("Note: This token has already expired.")

            elif command == 'logout':
                if auth_manager.get_session_token():
                    auth_manager.logout()
                    print("Logged out and session cleared.")
                else:
                    print("No active session to logout from.")

            elif command == 'save':
                if auth_manager._user_credentials:
                    auth_manager.save_to_json("credentials.json")
                    print("Credentials saved to credentials.json")
                else:
                    print("No credentials to save.")

            elif command == 'load':
                try:
                    auth_manager.load_from_json("credentials.json")
                    print("Credentials loaded from credentials.json")
                except FileNotFoundError:
                    print("Credentials file not found.")
                except json.JSONDecodeError:
                    print("Error reading credentials file. Please check file integrity.")

            elif command == 'quit':
                print("Exiting the program.")
                break

            else:
                print("Unknown command. Please try again.")

    except AuthenticationError as e:
        logger.error(f"Authentication error: {e}")
        print(f"Authentication error: {e}. Please check your credentials or try again later.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"An unexpected error occurred: {e}")