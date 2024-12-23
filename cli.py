import os
import asyncio
from getpass import getpass
from typing import List, Dict
import argparse
import signal
import sys
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter

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
    uid = input("Enter MangaDex UID: ")
    password = getpass("Enter MangaDex Password: ")
    return uid, password


def menu():
    """Display the main menu for CLI interaction."""
    print("\n--- MangaDex CLI Menu ---")
    print("1. Search Manga")
    print("2. View User's List")
    print("3. Help")
    print("4. Exit")
    return input("Choose an option: ")


async def search_manga(auth_manager: AuthManager, api: MangaDexAPI, page: int = 1) -> List[Dict]:
    """Search for manga based on various criteria with pagination."""
    query = input("Enter manga name/title, author, or tag (separate tags with commas): ")
    search_type = input("Search by (name/title/author/tag/word): ").lower()
    if search_type == "tag":
        tags = [tag.strip() for tag in query.split(',')]
        results = api.search_manga(tags=tags, page=page)
    else:
        results = api.search_manga(**{search_type: query}, page=page)

    for i, manga in enumerate(results, 1):
        print(f"{i}. {manga['title']} - {manga['manga_id']}")

    if len(results) == config.MAX_RESULTS_PER_PAGE:  # Assuming this config exists in Config
        if input("Do you want to see the next page? (y/n): ").lower() == 'y':
            return await search_manga(auth_manager, api, page + 1)
    return results


async def download_content(api: MangaDexAPI, downloader: ImageDownloader, data_storage: DataStorage, manga: Dict,
                           chapter_id: str, format_choice: str, test_mode: bool = False):
    """Download manga content in specified format."""
    try:
        chapter_data = api.get_chapter_details(chapter_id)
        if not chapter_data:
            logger.error(f"Chapter {chapter_id} not found.")
            print(f"Chapter {chapter_id} not found.")
            return

        image_urls = api.get_chapter_images(chapter_id)
        if not test_mode:
            if format_choice == '.pdf':
                batch_data = [{'urls': image_urls, 'pdf_name': f"{manga['title']}_Chapter_{chapter_data['chapter']}"}]
                results = await downloader.process_batch_async(batch_data)
                file_path = results[0]['pdf_path'] if results[0]["success"] else "Failed to create PDF"
            else:  # Assuming '.png' for simplicity
                file_path = os.path.join(downloader.output_path, f"{manga['title']}_Chapter_{chapter_data['chapter']}")
                os.makedirs(file_path, exist_ok=True)
                for i, url in enumerate(image_urls):
                    path = os.path.join(file_path, f"page_{i + 1}.png")
                    if not os.path.exists(path):
                        await downloader._download_image_async(url, f"page_{i + 1}.png", os.path.dirname(path), [])
                file_path = file_path if os.path.exists(file_path) else "Failed to download PNGs"

            print(f"File(s) available at: {file_path}")
            # Store in database if download was successful
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


def help_menu():
    """Display help information about CLI usage."""
    print("Usage:")
    print("  - Search Manga: Search for manga by various criteria.")
    print("  - View User's List: View and manage your personal manga list.")
    print("  - Help: Show this help message.")
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
    downloader = ImageDownloader()
    data_storage = DataStorage()

    session = PromptSession(history=FileHistory('cli_history.txt'), auto_suggest=AutoSuggestFromHistory(),
                            completer=WordCompleter(['search', 'view', 'help', 'exit']))

    while True:
        try:
            choice = session.prompt('Choose an option: ')
            if choice == '1' or choice.lower() == 'search':
                mangas = await search_manga(auth_manager, api)
                selection = int(input("Select a manga by number: ")) - 1
                if 0 <= selection < len(mangas):
                    selected_manga = mangas[selection]
                    chapters = api.get_manga_chapters(selected_manga['manga_id'])
                    for j, chapter in enumerate(chapters, 1):
                        print(f"{j}. Chapter {chapter['chapter_number']} - {chapter['title']}")

                    chapter_choice = int(input("Select a chapter by number: ")) - 1
                    if 0 <= chapter_choice < len(chapters):
                        chapter_id = chapters[chapter_choice]['chapter_id']
                        format_choice = input("Download as ('.pdf' or '.png'): ")
                        await download_content(api, downloader, data_storage, selected_manga, chapter_id, format_choice, test_mode)
                    else:
                        print("Invalid chapter selection.")
                else:
                    print("Invalid manga selection.")
            elif choice == '2' or choice.lower() == 'view':
                user_list = api.get_user_list()
                for item in user_list:
                    print(f"{item['title']} - {item['manga_id']}")
                # Here you would implement functionality to download from the user's list or manage it
            elif choice == '3' or choice.lower() == 'help':
                help_menu()
            elif choice == '4' or choice.lower() == 'exit':
                print("Exiting the program.")
                break
            else:
                print("Invalid option, please try again.")
        except ValueError:
            logger.error("Invalid input, please enter a number.")
            print("Invalid input, please enter a number.")
        except KeyboardInterrupt:
            print("\nCaught keyboard interrupt, attempting graceful exit...")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            print(f"An unexpected error occurred. Please try again or check the logs for more details.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MangaDex CLI with enhanced features.")
    parser.add_argument("--test", action="store_true", help="Run in test mode, no actual downloads.")
    args = parser.parse_args()

    asyncio.run(main(test_mode=args.test))