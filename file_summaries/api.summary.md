api.py contents:
Classes:

    MangaDexAPIError
        Base exception class for MangaDex API-related errors.
    RateLimitExceededError
        Custom exception for when the rate limit of the API is exceeded.
    APIChangeError
        Custom exception for handling unexpected changes in the API.
    MangaDexAPI
        Main class that handles all interactions with the MangaDex API. It includes:
            Authentication management
            API request handling with rate limiting and error checking
            Methods for various API operations like searching for manga, fetching chapters, etc.


Methods of MangaDexAPI:

    init(self, auth_manager: AuthManager)
        Initializes the API handler with an authentication manager, sets up the base URL, configures caching, and other settings from environment variables.
    _validate_id(self, id_value: str)
        Validates that an ID string is in the correct format (alphanumeric).
    _make_request(self, endpoint: str, params: Dict[str, Any] = {}) -> Dict[str, Any]
        Makes an HTTP GET request to the MangaDex API, handles rate limiting, logs performance, and deals with errors like authentication issues or rate limiting.
    _parse_manga_data(self, manga: Dict[str, Any]) -> Dict[str, Any]
        Parses raw manga data from the API response into a more usable format, extracting IDs, titles, authors, etc.
    _get_all_results(self, endpoint: str, params: Dict[str, Any], per_page: int = 100) -> List[Dict[str, Any]]
        Fetches all results from an endpoint using pagination, including caching to avoid repeated calls for the same data.
    search_manga(self, query: Optional[str] = None, author: Optional[str] = None, tags: Optional[List[str]] = None, excluded_tags: Optional[List[str]] = None, title: Optional[str] = None) -> List[Dict[str, Any]]
        Searches for manga based on various criteria like query, author, tags, with support for wildcard searches.
    get_chapter_details(self, chapter_id: str) -> Dict[str, Any]
        Retrieves detailed information about a specific chapter given its ID.
    get_chapter_images(self, chapter_id: str) -> List[str]
        Gets the URLs for images of a chapter, allowing for different quality preferences.
    health_check(self) -> bool
        Checks if the MangaDex API is responding by attempting a 'ping' request.


Functions:

    rate_limited_request(func)
        A decorator function that applies rate limiting to API calls, ensuring no more than the specified number of requests are made per second.


These functions and classes together form a comprehensive interface for interacting with the MangaDex API, providing functionality for authentication, data retrieval, and error handling.
