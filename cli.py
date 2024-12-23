import os
import asyncio
from getpass import getpass
from typing import List, Dict

from api import MangaDexAPI
from auth import AuthManager
from data_storage import DataStorage
from download import ImageDownloader


def prompt_for_credentials():
    """Prompt user for MangaDex credentials."""
    uid = input("Enter MangaDex UID: ")
    password = getpass("Enter MangaDex Password: ")
    return uid, password


def menu():
    """Display the main menu for CLI interaction."""
    print("\n--- MangaDex CLI Menu ---")
    print("1. Search Manga")
    print("2. View User's List")
    print("3. Exit")
    return input("Choose an option: ")


def search_manga(auth_manager: AuthManager, api: MangaDexAPI) -> List[Dict]:
    """Search for manga based on various criteria."""
    query = input("Enter manga name/title, author, or tag (separate tags with commas): ")
    search_type = input("Search by (name/title/author/tag/word): ").lower()
    if search_type == "tag":
        tags = [tag.strip() for tag in query.split(',')]
        results = api.search_manga(tags=tags)
    else:
        results = api.search_manga(**{search_type: query})
    return results


async def download_content(api: MangaDexAPI, downloader: ImageDownloader, data_storage: DataStorage, manga: Dict,
                           chapter_id: str, format_choice: str):
    """Download manga content in specified format."""
    chapter_data = api.get_chapter_details(chapter_id)
    if not chapter_data:
        print(f"Chapter {chapter_id} not found.")
        return

    image_urls = api.get_chapter_images(chapter_id)
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


async def main():
    auth_manager = AuthManager()
    uid, password = prompt_for_credentials()
    auth_manager.authenticate_with_credentials(uid, password)
    api = MangaDexAPI(auth_manager)
    downloader = ImageDownloader()
    data_storage = DataStorage()

    while True:
        choice = menu()
        if choice == '1':
            mangas = search_manga(auth_manager, api)
            for i, manga in enumerate(mangas, 1):
                print(f"{i}. {manga['title']} - {manga['manga_id']}")
            selection = int(input("Select a manga by number: ")) - 1
            selected_manga = mangas[selection]
            chapters = api.get_manga_chapters(selected_manga['manga_id'])
            for j, chapter in enumerate(chapters, 1):
                print(f"{j}. Chapter {chapter['chapter_number']} - {chapter['title']}")

            chapter_choice = int(input("Select a chapter by number: ")) - 1
            chapter_id = chapters[chapter_choice]['chapter_id']
            format_choice = input("Download as ('.pdf' or '.png'): ")
            await download_content(api, downloader, data_storage, selected_manga, chapter_id, format_choice)

        elif choice == '2':
            user_list = api.get_user_list()
            for item in user_list:
                print(f"{item['title']} - {item['manga_id']}")
            # Here you would implement functionality to download from the user's list

        elif choice == '3':
            print("Exiting the program.")
            break
        else:
            print("Invalid option, please try again.")


if __name__ == "__main__":
    asyncio.run(main())