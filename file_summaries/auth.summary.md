auth.py contents:
Classes:

    AuthenticationError
        A custom exception class for handling authentication-related errors.
    AuthManager
        Manages authentication for interacting with the MangaDex API. It handles:
            User authentication via username/password
            Session token management
            Storage and retrieval of encrypted credentials
            Logout functionality
            Token expiry checks


Methods of AuthManager:

    init
        Initializes the AuthManager with empty session token, credentials, and token expiry.
    authenticate_with_credentials(username: str, password: str) -> str
        Authenticates a user with MangaDex using username and password, returning a session token if successful.
    logout()
        Logs out from MangaDex by sending a logout request and clearing local authentication data.
    is_token_expired() -> bool
        Checks if the session token has expired based on the stored expiry time.
    store_user_credentials(username: str, password: str)
        Stores encrypted user credentials, validating the format of username and password before storage.
    get_decrypted_password(username: str) -> Optional[str]
        Retrieves and decrypts the stored password for a given username.
    get_session_token() -> Optional[str]
        Returns the current session token if it hasn't expired, otherwise returns None.
    save_to_json(file_path: str)
        Saves user credentials to a JSON file, excluding the session token for security.
    load_from_json(file_path: str)
        Loads user credentials from a JSON file, encrypting them back using the current key.
    validate_username(username: str) -> bool
        Validates if the username format is acceptable (alphanumeric, length between 3-20).
    validate_password(password: str) -> bool
        Validates if the password meets certain criteria (at least 8 characters, contains a digit and a letter).


Functions:

    validate_input(input_str: str) -> bool
        Checks if the input is a non-empty string.


Main Execution Block (if __name__ == "__main__":):

    Contains an interactive loop for user commands:
        'login': Prompts for UID and password to authenticate with MangaDex.
        'logout': Logs out if there's an active session.
        'save': Saves credentials to a JSON file if credentials exist.
        'load': Loads credentials from a JSON file.
        'quit': Exits the program.


This structure provides a comprehensive authentication system for interfacing with MangaDex, with security considerations like encryption, token management, and user input validation.
