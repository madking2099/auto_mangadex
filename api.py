import requests
import os
import time
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import logging
from ratelimit import limits, sleep_and_retry
from cachetools import TTLCache
from cryptography.fernet import Fernet
from auth import AuthenticationError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Check for .env file and handle environment variables
env_file = ".env"
if os.path.exists(env_file):
    logger.info(f"Loading environment variables from {env_file}")
    load_dotenv(env_file)
else:
    logger.info(f"{env_file} not found, generating new ENCRYPTION_KEY")
    key = Fernet.generate_key().decode()  # Decode to string for .env file
    with open(env_file, 'w') as f:
        f.write(f"ENCRYPTION_KEY={key}\n")
        f.write(f"MANGADEX_BASE_URL=https://api.mangadex.org/\n")
        f.write(f"CACHE_TTL=300\n")  # Default to 5 minutes
        f.write(f"MAX_RETRIES=3\n")
        f.write(f"MAX_RESPONSE_TIME=10.0\n")
        f.write(f"IMAGE_QUALITY=data\n")
        f.write(f"RATE_LIMIT_CALLS=2\n")
    load_dotenv(env_file)

# Ensure all necessary environment variables are set
if not os.environ.get('MANGADEX_BASE_URL'):
    with open(env_file, 'a') as f:
        f.write(f"MANGADEX_BASE_URL=https://api.mangadex.org/\n")
if not os.environ.get('CACHE_TTL'):
    with open(env_file, 'a') as f:
        f.write(f"CACHE_TTL=300\n")  # Default to 5 minutes
if not os.environ.get('MAX_RETRIES'):
    with open(env_file, 'a') as f:
        f.write(f"MAX_RETRIES=3\n")
if not os.environ.get('MAX_RESPONSE_TIME'):
    with open(env_file, 'a') as f:
        f.write(f"MAX_RESPONSE_TIME=10.0\n")
if not os.environ.get('IMAGE_QUALITY'):
    with open(env_file, 'a') as f:
        f.write(f"IMAGE_QUALITY=data\n")
if not os.environ.get('RATE_LIMIT_CALLS'):
    with open(env_file, 'a') as f:
        f.write(f"RATE_LIMIT_CALLS=2\n")
if not os.environ.get('ENCRYPTION_KEY'):
    key = Fernet.generate_key().decode()  # Decode to string for .env file
    with open(env_file, 'a') as f:
        f.write(f"ENCRYPTION_KEY={key}\n")
    load_dotenv(env_file)

# Reload .env to ensure new additions are in environment
load_dotenv(env_file)

# Rate limiting setup - now uses environment variable for rate limit
rate_limit_calls = int(os.environ.get('RATE_LIMIT_CALLS', 2))  # Default to 2 if not set or invalid


@sleep_and_retry
@limits(calls=rate_limit_calls, period=1)
def rate_limited_request(func):
    return func()


class MangaDexAPIError(Exception):
    """Base exception for MangaDex API errors."""
    pass


class RateLimitExceededError(MangaDexAPIError):
    """Exception raised when rate limit is exceeded."""
    pass


class APIChangeError(MangaDexAPIError):
    """Exception for unexpected API changes."""
    pass


class MangaDexAPI:
    """
    Handles interactions with the MangaDex API for various operations with rate limiting.

    This class manages:
    - Manga and chapter searches
    - Image URL retrieval
    - User data and custom lists
    - Tag information
    - Cover art
    - Group details
    - Caching and retry logic
    - Dynamic configuration via environment variables
    """

    def __init__(self, auth_manager):
        """
        Initialize with an AuthManager for handling authenticated requests.

        Args:
            auth_manager (AuthManager): An instance of AuthManager for authentication.
        """
        self.auth_manager = auth_manager
        self.base_url = os.environ.get('MANGADEX_BASE_URL', 'https://api.mangadex.org/')
        self.cache_ttl = int(os.environ.get('CACHE_TTL', 300))  # Default to 5 minutes
        self.max_retries = int(os.environ.get('MAX_RETRIES', 3))
        self.max_response_time = float(os.environ.get('MAX_RESPONSE_TIME', 10.0))  # in seconds
        self.cache = TTLCache(maxsize=1000, ttl=self.cache_ttl)  # Using cachetools for TTL

    def _validate_id(self, id_value: str):
        """
        Validates that the provided ID is in a correct format.

        Args:
            id_value (str): The ID to validate.

        Raises:
            ValueError: If the ID is not valid.
        """
        if not isinstance(id_value, str) or not id_value.isalnum():
            raise ValueError(f"Invalid ID format: {id_value}")

    def _make_request(self, endpoint: str, params: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Makes an HTTP request to the MangaDex API with rate limiting, error handling, and retry logic.

        Args:
            endpoint (str): The API endpoint to hit.
            params (Dict[str, Any]): Parameters for the API call.

        Returns:
            Dict[str, Any]: The JSON response from the API.

        Raises:
            AuthenticationError: If there's an issue with authentication.
            RateLimitExceededError: If rate limit is exceeded.
            MangaDexAPIError: For other API errors.
            requests.exceptions.RequestException: For network or API errors.
        """
        if self.auth_manager.is_token_expired():
            logger.error("Session token expired. Please re-authenticate.")
            raise AuthenticationError("Session token expired. Please re-authenticate.")

        headers = {"Authorization": f"Bearer {self.auth_manager.get_session_token()}"}

        @rate_limited_request
        def do_request(retry_count=0):
            try:
                start_time = time.time()
                response = requests.get(f"{self.base_url}{endpoint}", headers=headers, params=params)
                response.raise_for_status()
                response_time = time.time() - start_time
                response_size = len(response.content)
                if response_time > self.max_response_time:
                    logger.warning(
                        f"API call to {endpoint} took {response_time:.2f} seconds, exceeding max response time.")
                logger.info(
                    f"API call to {endpoint} completed in {response_time:.2f} seconds, {response_size} bytes transferred.")
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"API Response: {response.json()}")
                return response.json()
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    logger.warning(
                        f"Rate limit exceeded on {endpoint}. Retrying after delay. Current limit: {rate_limit_calls} calls per second.")
                    raise RateLimitExceededError(f"Rate limit exceeded on {endpoint}")
                elif response.status_code == 401:
                    logger.error(f"Authentication failed on {endpoint}. Check credentials or token.")
                    raise AuthenticationError("Authentication failed. Please check your credentials or token.")
                else:
                    error_json = response.json() if response.headers.get('content-type') == 'application/json' else {}
                    error_message = error_json.get('message', f"HTTP Error {response.status_code}")
                    logger.error(f"Error on {endpoint}: {error_message}")
                    raise MangaDexAPIError(error_message)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request Exception on {endpoint}: {e}")
                if retry_count < self.max_retries:
                    logger.warning(f"Retrying request to {endpoint}")
                    return do_request(retry_count + 1)
                raise

        return do_request()

    def _parse_manga_data(self, manga: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses manga data to extract relevant IDs and summaries.

        Args:
            manga (Dict[str, Any]): Raw manga data from API.

        Returns:
            Dict[str, Any]: Parsed data with IDs and summaries.
        """
        attributes = manga['attributes']
        return {
            'manga_id': manga['id'],
            'title': attributes.get('title', {}).get('en', "No English title"),
            'author_ids': [rel['id'] for rel in manga['relationships'] if rel['type'] == 'author'],
            'artist_ids': [rel['id'] for rel in manga['relationships'] if rel['type'] == 'artist'],
            'description': attributes.get('description', {}).get('en', "No description"),
            'tags': [tag['id'] for tag in attributes.get('tags', [])],
            'cover_id': next((rel['id'] for rel in manga['relationships'] if rel['type'] == 'cover_art'), None),
            'last_chapter': attributes.get('lastChapter', None)
        }

    def _get_all_results(self, endpoint: str, params: Dict[str, Any], per_page: int = 100) -> List[Dict[str, Any]]:
        """
        Fetches all results using pagination, with caching.

        Args:
            endpoint (str): The API endpoint.
            params (Dict[str, Any]): Initial parameters for the query.
            per_page (int): Number of items to fetch per request.

        Returns:
            List[Dict[str, Any]]: All results from the endpoint.
        """
        all_results = []
        params['limit'] = per_page
        offset = 0
        cache_key = f"{endpoint}{params}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        while True:
            params['offset'] = offset
            results = self._make_request(endpoint, params).get('data', [])
            if not results:
                break
            all_results.extend(results)
            offset += per_page
            if len(results) < per_page:  # If we got fewer results than requested, we've reached the end
                break

        self.cache[cache_key] = all_results  # Cache the results
        return all_results

    async def search_manga(self,
                           query: Optional[str] = None,
                           author: Optional[str] = None,
                           tags: Optional[List[str]] = None,
                           excluded_tags: Optional[List[str]] = None,
                           language: Optional[str] = None,
                           page: int = 1) -> List[Dict[str, Any]]:
        """
        Search for manga by various criteria with support for wildcard searches.

        Examples:
            await api.search_manga(query="one piece")
            await api.search_manga(author="Oda")

        Args:
            query (str): General keyword search, supports wildcards.
            author (str): Search by author name, supports wildcards.
            tags (List[str]): List of tag IDs to include.
            excluded_tags (List[str]): List of tag IDs to exclude.
            language (str): Language code for filtering results.
            page (int): The page number for pagination.

        Returns:
            List[Dict[str, Any]]: List of manga matching the search criteria, with extracted IDs and summaries.
        """
        if query and not isinstance(query, str):
            raise ValueError("Query must be a string")
        if author and not isinstance(author, str):
            raise ValueError("Author must be a string")
        if tags and not all(isinstance(tag, str) for tag in tags):
            raise ValueError("All tags must be strings")
        if excluded_tags and not all(isinstance(tag, str) for tag in excluded_tags):
            raise ValueError("All excluded tags must be strings")
        if language and not isinstance(language, str):
            raise ValueError("Language must be a string")

        params = {}
        if query:
            params['title'] = f"%{query}%"  # Wildcard support
        if author:
            params['authors'] = f"%{author}%"  # Wildcard support for author search
        if tags:
            params['includedTags[]'] = tags
        if excluded_tags:
            params['excludedTags[]'] = excluded_tags
        if language:
            params['availableTranslatedLanguage[]'] = language
        params['offset'] = (page - 1) * 100  # Assuming 100 items per page

        all_manga = self._get_all_results("manga", params)
        return [self._parse_manga_data(manga) for manga in all_manga]

    async def get_chapter_details(self, chapter_id: str) -> Dict[str, Any]:
        """
        Retrieve chapter details.

        Example:
            chapter_details = await api.get_chapter_details("some-chapter-id")

        Args:
            chapter_id (str): The ID of the chapter to fetch details for.

        Returns:
            Dict[str, Any]: Chapter details including manga_id, title, volume, and chapter number.
        """
        self._validate_id(chapter_id)
        chapter = self._make_request(f"chapter/{chapter_id}")
        chapter_data = chapter['data']['attributes']
        return {
            'chapter_id': chapter['data']['id'],
            'manga_id': next(rel['id'] for rel in chapter['data']['relationships'] if rel['type'] == 'manga'),
            'title': chapter_data.get('title', "No title"),
            'volume': chapter_data.get('volume', None),
            'chapter': chapter_data.get('chapter', None),
            'hash': chapter_data.get('hash', None)
        }

    async def get_chapter_images(self, chapter_id: str) -> List[str]:
        """
        Retrieve the image URLs for a chapter's pages.

        Example:
            image_urls = await api.get_chapter_images("some-chapter-id")

        Args:
            chapter_id (str): The ID of the chapter whose images are needed.

        Returns:
            List[str]: URLs of images for the chapter.

        Raises:
            AuthenticationError: If there's an issue with authentication.
            requests.exceptions.RequestException: For network or API errors.
        """
        self._validate_id(chapter_id)
        server_info = self._make_request(f"at-home/server/{chapter_id}")
        if 'baseUrl' not in server_info:
            raise AuthenticationError("Failed to get at-home server info for chapter")

        base_url = server_info['baseUrl']
        chapter_hash = server_info['chapter']['hash']
        quality_type = os.environ.get('IMAGE_QUALITY', 'data')  # Environment variable for quality preference
        data_quality = server_info['chapter'][quality_type]

        image_urls = []
        for filename in data_quality:
            image_urls.append(f"{base_url}/{quality_type}/{chapter_hash}/{filename}")

        return image_urls

    async def get_manga_chapters(self, manga_id: str) -> List[Dict[str, Any]]:
        """
        Get chapters for a specific manga.

        Args:
            manga_id (str): The ID of the manga.

        Returns:
            List[Dict[str, Any]]: A list of chapter dictionaries.
        """
        self._validate_id(manga_id)
        return self._get_all_results(f"manga/{manga_id}/feed", params={"limit": 100})

    async def get_user_list(self) -> List[Dict[str, Any]]:
        """
        Retrieve the user's list from MangaDex API.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing manga information from user's list.
        """
        if self.auth_manager.is_token_expired():
            raise AuthenticationError("Session token has expired. Please re-authenticate.")

        response = self._make_request("user/follows/manga", params={"limit": 100})  # Adjust limit as needed
        return response.get('data', [])