import os
import asyncio
from getpass import getpass
from typing import List, Dict
import argparse
import signal
import sys
import json
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
from progress.bar import IncrementalBar
import aiohttp

from api import MangaDexAPI, MangaDexAPIError
from auth import AuthManager
from data_storage import DataStorage
from download import ImageDownloader, Config

# Setup logging
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from .env
config = Config()
config.ensure_env_variables()


def prompt_for_credentials():
    """Prompt user for MangaDex credentials securely."""
    uid =  input("Enter MangaDex UID: ")
    password = getpass("Enter MangaDex Password: ")
    return uid, password


def menu():
    """Display the main menu for CLI interaction."""
    print("\n--- MangaDex CLI Menu ---")
    print("1. Search Manga")
    print("2. View User's List")
    print("3. Help")
    print("4. Set Output Directory")
    print("5. Exit")
    return input("Choose an option: ")


def load_user_config(config_path: str) -> dict:
    """Load user configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_user_config(config_path: str, config_data: dict):
    """Save user configuration to JSON file."""
    with open(config_path, 'w') as f:
        json.dump(config_data, f)


def log_user_action(action: str, details: str):
    """Log user actions for auditing or debugging."""
    logger.info(f"User Action: {action} - Details: {details}")


async def retry_on_failure(func, *args, max_retries=3, delay=2):
    """Retry a function with potential network issues."""
    for attempt in range(max_retries):
        try:
            return await func(*args)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds: {e}")
            await asyncio.sleep(delay)


class ProgressBar:
    def __init__(self):
        self.bar = None

    def start(self, message, max_value):
        self.bar = IncrementalBar(message, max=max_value)

    def next(self):
        if self.bar:
            self.bar.next()

    def finish(self):
        if self.bar:
            self.bar.finish()
        self.bar = None


progress_bar = ProgressBar()


async def search_manga(auth_manager: AuthManager, api: MangaDexAPI, page: int = 1) -> List[Dict]:
    """Search for manga based on various criteria with advanced filters."""
    progress_bar.start('Searching...', 100)  # Adjust max based on expected results
    query = input("Enter manga name/title, author, or tag (separate tags with commas): ")
    search_type = input("Search by (name/title/author/tag/word): ").lower()
    exclude_tags = input("Enter tags to exclude (comma-separated, press enter for none): ").split(',')
    language = input("Enter language code (e.g., 'en' for English, leave blank for all): ").strip() or None

    log_user_action("Search",
                    f"Query: {query}, Type: {search_type}, Excluded Tags: {exclude_tags}, Language: {language}")

    if search_type == "tag":
        tags = [tag.strip() for tag in query.split(',')]
        results = await retry_on_failure(api.search_manga, tags=tags, excluded_tags=exclude_tags, language=language,
                                         page=page)
    else:
        results = await retry_on_failure(api.search_manga, **{search_type: query}, excluded_tags=exclude_tags,
                                         language=language, page=page)

    for _ in range(100):  # Simulated progress, adjust based on actual results
        progress_bar.next()

    progress_bar.finish()

    for i, manga in enumerate(results, 1):
        print(f"{i}. {manga['title']} - {manga['manga_id']}")

    if len(results) == config.MAX_RESULTS_PER_PAGE:  # Assuming this config exists in Config
        if input("Do you want to see the next page? (y/n): ").lower() == 'y':
            return await search_manga(auth_manager, api, page + 1)
    return results


async def download_content(api: MangaDexAPI, downloader: ImageDownloader, data_storage: DataStorage, manga: Dict,
                           chapter_id: str, format_choice: str, test_mode: bool = False):
    """Download manga content in specified format with possibility to restart failed downloads."""
    try:
        chapter_data = await retry_on_failure(api.get_chapter_details, chapter_id)
        if not chapter_data:
            logger.error(f"Chapter {chapter_id} not found.")
            print(f"Chapter {chapter_id} not found.")
            return

        image_urls = await retry_on_failure(api.get_chapter_images, chapter_id)

        partial_dir = os.path.join(downloader.output_path,
                                   f"partial_{manga['title']}_Chapter_{chapter_data['chapter']}")
        os.makedirs(partial_dir, exist_ok=True)

        existing_files = {f for f in os.listdir(partial_dir) if f.startswith('page_') and f.endswith('.png')}
        start_from = max([int(f.split('_')[1].split('.')[0]) for f in existing_files], default=0)

        progress_bar.start('Downloading...', len(image_urls))
        if format_choice == '.pdf':
            batch_data = [{'urls': image_urls, 'pdf_name': f"{manga['title']}_Chapter_{chapter_data['chapter']}"}]
            results = await retry_on_failure(downloader.process_batch_async, batch_data)
            for i in range(start_from, len(image_urls)):
                progress_bar.next()
            file_path = results[0]['pdf_path'] if results[0]["success"] else "Failed to create PDF"
        else:  # Assuming '.png' for simplicity
            file_path = partial_dir
            for i, url in enumerate(image_urls[start_from:], start=start_from):
                path = os.path.join(partial_dir, f"page_{i + 1}.png")
                if not os.path.exists(path):
                    await retry_on_failure(downloader._download_image_async, url, f"page_{i + 1}.png", partial_dir, [])
                progress_bar.next()
            file_path = file_path if os.path.exists(file_path) else "Failed to download PNGs"

        progress_bar.finish()

        if not test_mode:
            log_user_action("Download",
                            f"Manga: {manga['title']}, Chapter: {chapter_data['chapter']}, Format: {format_choice}, Path: {file_path}")
            print(f"File(s) available at: {file_path}")
            if format_choice == '.pdf':
                data_storage.store_file(downloader.output_path, manga['manga_id'], file_path)
        else:
            print("Test mode: No actual download performed.")
    except MangaDexAPIError as e:
        logger.error(f"API Error: {e}")
        print(f"An error occurred while downloading: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"An unexpected error occurred: {e}")


async def interactive_search(api: MangaDexAPI, auth_manager: AuthManager, downloader: ImageDownloader,
                             data_storage: DataStorage, config: dict, test_mode: bool = False):
    """Provide an interactive mode for searching and downloading."""
    while True:
        mangas = await search_manga(auth_manager, api)
        action = input("Do you want to (d)ownload, (s)earch again, or (q)uit to main menu? ").lower()
        if action == 'd':
            selection = int(input("Select a manga by number: ")) - 1
            if 0 <= selection < len(mangas):
                selected_manga = mangas[selection]
                chapters = await retry_on_failure(api.get_manga_chapters, selected_manga['manga_id'])
                for j, chapter in enumerate(chapters, 1):
                    print(f"{j}. Chapter {chapter['chapter_number']} - {chapter['title']}")
                chapter_choice = int(input("Select a chapter by number: ")) - 1
                if 0 <= chapter_choice < len(chapters):
                    await retry_on_failure(download_content, api, downloader, data_storage, selected_manga,
                                           chapters[chapter_choice]['chapter_id'], config['default_format'], test_mode)
        elif action == 's':
            continue
        elif action == 'q':
            break
        else:
            print("Invalid choice.")


def help_menu():
    """Display help information about CLI usage."""
    print("Usage:")
    print("  - Search Manga: Search for manga by various criteria.")
    print("  - View User's List: View and manage your personal manga list.")
    print("  - Help: Show this help message.")
    print("  - Set Output Directory: Change where files are downloaded.")
    print("  - Exit: Exit the application.")
    print("Commands:")
    print("  - Press Ctrl+C once to gracefully exit.")
    print("  - Press Ctrl+C twice to force exit.")
    print("For more detailed usage, please refer to README.md")


async def main(test_mode: bool = False):
    interrupt_count = 0

    def signal_handler(signum, frame):
        nonlocal interrupt_count
        interrupt_count += 1
        if interrupt_count > 1 or not asyncio.get_event_loop().is_running():
            print("\nForcing exit...")
            sys.exit(1)
        else:
            print("\nPress Ctrl+C again to force exit. Gracefully exiting...")
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, signal_handler)

    auth_manager = AuthManager()
    uid, password = prompt_for_credentials()
    try:
        auth_manager.authenticate_with_credentials(uid, password)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        print(f"Authentication failed. Please check your credentials.")
        return

    api = MangaDexAPI(auth_manager)
    data_storage = DataStorage()
    user_config = data_storage.get_user_config()
    if not user_config:
        if input("Do you want to save configurations? (y/n): ").lower() == 'y':
            default_config = {'default_format': '.pdf', 'max_concurrent_downloads': 2}
            data_storage.save_user_config(default_config)
        else:
            default_config = {'default_format': '.pdf', 'max_concurrent_downloads': 2}
            user_config = default_config  # Use in-memory config

    downloader = ImageDownloader(output_path=user_config.get('output_directory', '.'))

    session = PromptSession(history=FileHistory('cli_history.txt'), auto_suggest=AutoSuggestFromHistory(),
                            completer=WordCompleter(['search', 'view', 'help', 'exit', 'set']))

    while True:
        try:
            choice = session.prompt('Choose an option: ')
            if choice == '1' or choice.lower() == 'search':
                if input("Enter interactive mode? (y/n): ").lower() == 'y':
                    await interactive_search(api, auth_manager, downloader, data_storage, user_config, test_mode)
                else:
                    mangas = await search_manga(auth_manager, api)
                    selection = int(input("Select a manga by number: ")) - 1
                    if 0 <= selection < len(mangas):
                        selected_manga = mangas[selection]
                        chapters = await retry_on_failure(api.get_manga_chapters, selected_manga['manga_id'])
                        for j, chapter in enumerate(chapters, 1):
                            print(f"{j}. Chapter {chapter['chapter_number']} - {chapter['title']}")

                        chapter_choice = int(input("Select a chapter by number: ")) - 1
                        if 0 <= chapter_choice < len(chapters):
                            await retry_on_failure(download_content, api, downloader, data_storage, selected_manga,
                                                   chapters[chapter_choice]['chapter_id'],
                                                   user_config['default_format'], test_mode)
                        else:
                            print("Invalid chapter selection.")
                    else:
                        print("Invalid manga selection.")
            elif choice == '2' or choice.lower() == 'view':
                user_list = await retry_on_failure(api.get_user_list)
                for item in user_list:
                    print(f"{item['title']} - {item['manga_id']}")
                # Here you would implement functionality to download from the user's list or manage it
            elif choice == '3' or choice.lower() == 'help':
                help_menu()
            elif choice == '4' or choice.lower() == 'set':
                new_dir = input("Enter new output directory: ")
                if os.path.isdir(new_dir):
                    downloader = ImageDownloader(output_path=new_dir)
                    user_config['output_directory'] = new_dir
                    data_storage.save_user_config(user_config)
                    print(f"Output directory set to: {new_dir}")
                else:
                    print("Directory does not exist. Output directory unchanged.")
            elif choice == '5' or choice.lower() == 'exit':
                print("Exiting the program.")
                break
            else:
                print("Invalid option, please try again.")
        except ValueError:
            logger.error("Invalid input, please enter a number.")
            print("Invalid input, please enter a number.")
            break
        except KeyboardInterrupt:
            print("\nCaught keyboard interrupt, attempting graceful exit...")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            print(f"An unexpected error occurred. Please try again or check the logs for more details.")
            break
        finally:
            tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MangaDex CLI with enhanced features.")
    parser.add_argument("--test", action="store_true", help="Run in test mode, no actual downloads.")
    args = parser.parse_args()

    asyncio.run(main(test_mode=args.test))