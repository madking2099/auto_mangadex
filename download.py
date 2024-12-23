import os
import requests
import shutil
import tempfile
from typing import List, Dict, Optional, Callable
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from io import BytesIO
from PIL import Image
import logging
from progress.bar import Bar
from requests.exceptions import RequestException
from time import sleep
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import threading
import traceback
import signal
import asyncio
import aiohttp
from pypdf import PdfReader, PdfWriter

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Configuration Class for Modular Configuration
class Config:
    def __init__(self):
        load_dotenv(".env")
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
        self.HTTP_TIMEOUT = int(os.getenv('HTTP_TIMEOUT', '10'))
        self.ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')
        self.MAX_CONCURRENT_DOWNLOADS = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '2'))
        self.PDF_PAGE_SIZE = os.getenv('PDF_PAGE_SIZE', 'letter').lower()
        if self.PDF_PAGE_SIZE == 'a4':
            self.PDF_PAGE_SIZE = (595.276, 841.89)  # A4 size in points
        else:  # default to letter
            self.PDF_PAGE_SIZE = (letter[0], letter[1])  # letter size in points
        self.PDF_CREATION_TIMEOUT = int(os.getenv('PDF_CREATION_TIMEOUT', '60'))  # seconds

    def ensure_env_variables(self):
        env_file = ".env"
        if not os.path.exists(env_file):
            with open(env_file, 'w') as f:
                f.write(f"MAX_RETRIES=3\n")
                f.write(f"HTTP_TIMEOUT=10\n")
                f.write(f"MAX_CONCURRENT_DOWNLOADS=2\n")
                f.write(f"PDF_PAGE_SIZE=letter\n")
                f.write(f"PDF_CREATION_TIMEOUT=60\n")
            load_dotenv(env_file)

        for var, value in [
            ('MAX_RETRIES', '3'),
            ('HTTP_TIMEOUT', '10'),
            ('MAX_CONCURRENT_DOWNLOADS', '2'),
            ('PDF_PAGE_SIZE', 'letter'),
            ('PDF_CREATION_TIMEOUT', '60'),
            ('ENCRYPTION_KEY', Fernet.generate_key().decode())
        ]:
            if not os.environ.get(var):
                with open(env_file, 'a') as f:
                    f.write(f"{var}={value}\n")
                load_dotenv(env_file)


config = Config()
config.ensure_env_variables()


class CustomBar(Bar):
    message = '%(percent)d%% %(current)d/%(total)d'
    fill = '#'
    suffix = '%(elapsed_td)s'


def signal_handler(signum, frame):
    logger.info("Received interrupt signal. Attempting to stop threads gracefully.")
    raise SystemExit


signal.signal(signal.SIGINT, signal_handler)


class PDFIntegrityError(Exception):
    """Custom exception for PDF integrity issues."""
    pass


class AsyncProgress:
    def __init__(self, total):
        self.total = total
        self.current = 0
        self.lock = asyncio.Lock()

    async def update(self, increment=1):
        async with self.lock:
            self.current += increment
            print(f"\rProgress: {self.current}/{self.total}", end="", flush=True)

    def close(self):
        print()  # New line to reset the progress indicator


class ImageDownloader:
    """
    Handles downloading images from URLs, converting them to PNG, and creating PDFs asynchronously.
    """

    def __init__(self, output_path: str = ".", progress_callback: Callable[[str], None] = None):
        self.output_path = output_path
        self.lock = threading.Lock()
        self.progress_callback = progress_callback
        self._cancel_event = asyncio.Event()

    def _update_progress(self, message: str):
        if self.progress_callback:
            with self.lock:
                self.progress_callback(message)

    def cancel_processing(self):
        """Set the cancellation event to stop ongoing downloads."""
        self._cancel_event.set()

    async def _check_image_quality_async(self, image_path: str) -> bool:
        """
        Asynchronously check if the image meets a basic quality standard (not corrupt, has content).
        """
        try:
            async with await asyncio.to_thread(Image.open, image_path) as img:
                img.verify()
                if img.size[0] < 10 or img.size[1] < 10:
                    return False
                return True
        except Exception as e:
            logger.error(f"Image at {image_path} might be corrupt or invalid: {e}")
            return False

    async def _download_image_async(self, url: str, filename: str, temp_dir: str, results: list):
        """
        Download an image asynchronously with retry logic and error handling.
        """
        if self._cancel_event.is_set():
            return

        async with aiohttp.ClientSession() as session:
            for attempt in range(config.MAX_RETRIES + 1):
                try:
                    async with session.get(url, timeout=config.HTTP_TIMEOUT) as response:
                        response.raise_for_status()
                        path = os.path.join(temp_dir, filename)
                        content = await response.read()
                        with open(path, 'wb') as f:
                            f.write(content)
                        if await self._check_image_quality_async(path):
                            results.append(path)
                        else:
                            logger.warning(f"Image from {url} does not meet quality standards, skipping.")
                            results.append(None)
                        return
                except aiohttp.ClientError as e:
                    logger.error(f"Client error downloading {url}: {e}")
                except asyncio.TimeoutError:
                    logger.error(f"Timeout occurred while downloading from {url}")
                except Exception as e:
                    logger.error(f"Unexpected error downloading {url}: {e}")

                if attempt == config.MAX_RETRIES:
                    logger.error(f"Failed to download image from {url} after {config.MAX_RETRIES} attempts")
                    results.append(None)
                else:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

    async def process_batch_async(self, batch_data: List[Dict[str, Any]], progress_bar: bool = True,
                                  max_batch_retries: int = 2):
        """
        Process a batch of manga chapters asynchronously with retry mechanism for the whole batch if there are failures.
        """
        if not batch_data:
            logger.info("No items to process in batch.")
            return []

        print(f"Processing {len(batch_data)} chapters...")
        self._update_progress(f"Starting batch process for {len(batch_data)} chapters")
        self._cancel_event.clear()

        for retry in range(max_batch_retries + 1):
            pdf_results = await self._process_batch_once_async(batch_data, progress_bar)
            failed_count = sum(1 for result in pdf_results if not result["success"])

            if self._cancel_event.is_set():
                logger.info("Batch processing cancelled by user.")
                return []

            if failed_count == 0:
                return pdf_results
            elif retry < max_batch_retries:
                logger.warning(
                    f"Batch processing failed for {failed_count} items. Retrying {retry + 1}/{max_batch_retries}...")
                self._update_progress(f"Retrying batch process due to {failed_count} failures")
            else:
                logger.error(f"Batch processing failed for {failed_count} items after {max_batch_retries} retries.")
                self._update_progress(f"Batch process failed after {max_batch_retries} retries")
                return pdf_results

    async def _process_batch_once_async(self, batch_data: List[Dict[str, Any]], progress_bar: bool = True):
        start_time = asyncio.get_event_loop().time()
        pdf_results = []

        for item in batch_data:
            logger.info(f"Starting download for {item['chapter_id']} of {item['manga_title']}")
            self._update_progress(f"Processing chapter {item['chapter_id']} of {item['manga_title']}")
            async with tempfile.TemporaryDirectory() as temp_dir:
                if progress_bar:
                    progress = AsyncProgress(len(item['image_urls']) * 2)

                image_paths = []
                sem = asyncio.Semaphore(config.MAX_CONCURRENT_DOWNLOADS)

                async def download_one_image(url, filename):
                    async with sem:
                        return await self._download_image_async(url, filename, temp_dir, image_paths)

                tasks = [download_one_image(url, f"image_{i:03d}.jpg") for i, url in enumerate(item['image_urls'])]
                await asyncio.gather(*tasks)

                png_paths = []
                for path in image_paths:
                    if path:
                        png_path = await asyncio.to_thread(self._convert_to_png, path)
                        if png_path:
                            png_paths.append(png_path)
                        if progress_bar:
                            await progress.update()

                if png_paths:
                    pdf_path = os.path.join(temp_dir, f"{item['manga_title']}_Chapter_{item['chapter_number']}.pdf")
                    success = await self._create_pdf_with_retry_async(png_paths, pdf_path, item)
                    if success:
                        try:
                            if await asyncio.to_thread(self._check_pdf_integrity, pdf_path):
                                final_pdf_path = os.path.join(self.output_path, os.path.basename(pdf_path))
                                shutil.move(pdf_path, final_pdf_path)
                                pdf_results.append({"pdf_path": final_pdf_path, "success": True})
                                logger.info(
                                    f"Successfully created PDF from {len(png_paths)} images for {item['chapter_id']}: {final_pdf_path}")
                                if progress_bar:
                                    await progress.update(len(item['image_urls']))  # For the PDF creation step
                                    progress.close()
                            else:
                                logger.error(f"PDF integrity check failed for {item['chapter_id']} at {pdf_path}")
                                pdf_results.append({"pdf_path": None, "success": False})
                        except PDFIntegrityError as e:
                            logger.error(f"PDF Integrity Check Error for {item['chapter_id']}: {e}")
                            pdf_results.append({"pdf_path": None, "success": False})
                    else:
                        logger.error(f"PDF creation failed for {item['chapter_id']} after retries")
                        pdf_results.append({"pdf_path": None, "success": False})
                else:
                    logger.warning(
                        f"No valid images for {item['chapter_id']} of {item['manga_title']}, skipping PDF creation.")
                    pdf_results.append({"pdf_path": None, "success": False})

                # Cleanup of leftover files in the temporary directory
                for file in os.listdir(temp_dir):
                    try:
                        os.remove(os.path.join(temp_dir, file))
                    except Exception as e:
                        logger.error(f"Failed to remove temporary file {file}: {e}")

        total_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"Batch processing completed in {total_time:.2f} seconds")
        self._update_progress(f"Batch processing completed in {total_time:.2f} seconds")
        return pdf_results

    async def _create_pdf_with_retry_async(self, image_paths: List[str], output_file: str, item: Dict[str, Any], max_retries: int = 2) -> bool:
        """
        Create PDF asynchronously with retry mechanism if creation fails, with a timeout for each attempt.
        """
        for attempt in range(max_retries + 1):
            try:
                task = asyncio.create_task(asyncio.to_thread(self._create_pdf, image_paths, output_file, item))
                done, pending = await asyncio.wait([task], timeout=config.PDF_CREATION_TIMEOUT)
                if task in done:
                    return task.result()
                else:
                    task.cancel()
                    raise TimeoutError("PDF creation timed out")
            except TimeoutError as e:
                logger.error(f"PDF creation for {item['chapter_id']} timed out after {config.PDF_CREATION_TIMEOUT} seconds: {e}")
            except Exception as e:
                if attempt == max_retries:
                    logger.error(f"PDF creation failed after {max_retries} retries for {output_file}: {e}")
                    return False
                logger.warning(f"PDF creation attempt {attempt + 1}/{max_retries} failed for {output_file}, retrying: {e}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        return False

    def _convert_to_png(self, image_path: str) -> Optional[str]:
        """
        Convert image to PNG, detecting input format automatically.
        """
        try:
            with Image.open(image_path) as img:
                current_format = img.format
                if current_format != 'PNG':
                    png_path = image_path.rsplit('.', 1)[0] + '.png'
                    img.save(png_path, 'PNG')
                    return png_path
                else:
                    return image_path
        except Exception as e:
            logger.error(f"Failed to convert {image_path} to PNG: {e}")
            return None

    def _create_pdf(self, image_paths: List[str], output_file: str, item: Dict[str, Any]):
        """
        Create a PDF from a list of image paths with improved logging and metadata.
        """
        c = canvas.Canvas(output_file, pagesize=config.PDF_PAGE_SIZE)
        width, height = config.PDF_PAGE_SIZE

        for image_path in image_paths:
            try:
                with Image.open(image_path) as img:
                    img_width, img_height = img.size
                    aspect = img_width / float(img_height)
                    if aspect > 1:  # landscape
                        new_height = height
                        new_width = new_height * aspect
                    else:
                        new_width = width
                        new_height = new_width / aspect
                    c.drawImage(image_path, (width - new_width) / 2, (height - new_height) / 2, new_width, new_height)
                    c.showPage()
                    logger.info(f"Added image {os.path.basename(image_path)} to PDF for {item['chapter_id']}")
            except IOError as e:
                logger.error(f"Failed to process image {os.path.basename(image_path)} for {item['chapter_id']}: {e}")

        c.save()
        self._add_pdf_metadata(output_file, item)
        logger.info(f"PDF created: {output_file} for {item['chapter_id']}")

    def _add_pdf_metadata(self, pdf_path: str, item: Dict[str, Any]):
        """
        Add metadata to PDF file based on manga data from api.py.
        """
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)

        writer.add_metadata({
            '/Title': f"{item.get('manga_title', 'Unknown Manga')} - Chapter {item.get('chapter_number', 'Unknown')}",
            '/Author': ', '.join(item.get('authors', [])),
            '/Subject': f"Chapter {item.get('chapter_number', 'Unknown')}",
            '/Keywords': f"Manga, {item.get('manga_title', '')}, {', '.join(item.get('tags', []))}",
            '/Creator': 'Your Application Name'
        })

        with open(pdf_path, "wb") as output_stream:
            writer.write(output_stream)
        logger.info(f"Metadata added to {pdf_path} for {item['chapter_id']}")

    def _check_pdf_integrity(self, pdf_path: str) -> bool:
        """
        Perform multiple checks to verify PDF integrity using internal Python methods.
        """
        if not self._check_pdf_header(pdf_path):
            raise PDFIntegrityError("PDF header check failed")
        if not self._check_pdf_trailer(pdf_path):
            raise PDFIntegrityError("PDF trailer check failed")
        try:
            with open(pdf_path, 'rb') as file:
                PdfReader(file)
            return True
        except Exception as e:
            raise PDFIntegrityError(f"PDF integrity check failed: {e}")

    def _check_pdf_header(self, pdf_path: str) -> bool:
        """
        Check if the PDF starts with the correct header.
        """
        with open(pdf_path, 'rb') as file:
            header = file.read(8)
        if header.startswith(b'%PDF-'):
            return True
        logger.error(f"PDF header check failed for {pdf_path}")
        return False

    def _check_pdf_trailer(self, pdf_path: str) -> bool:
        """
        Check for the presence of a PDF trailer.
        """
        with open(pdf_path, 'rb') as file:
            file.seek(0, os.SEEK_END)
            size = file.tell()
            file.seek(max(0, size - 2048))  # Look at the last 2048 bytes
            content = file.read()
            if b'%%EOF' in content:
                return True
            logger.error(f"PDF trailer check failed for {pdf_path}")
            return False

async def shutdown_async():
    """Gracefully shut down all asynchronous tasks."""
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    import asyncio

    downloader = ImageDownloader(progress_callback=lambda msg: print(msg))

    # Example batch data from api.py (simulated)
    batch_data = [
        {
            'chapter_id': 'chapter123',
            'chapter_number': '1',
            'manga_title': 'Manga Title 1',
            'authors': ['Author 1', 'Author 2'],
            'tags': ['Shounen', 'Action'],
            'image_urls': ["url_to_image1.jpg", "url_to_image2.jpg"]
        },
        {
            'chapter_id': 'chapter456',
            'chapter_number': '2',
            'manga_title': 'Manga Title 2',
            'authors': ['Author 3'],
            'tags': ['Fantasy', 'Adventure'],
            'image_urls': ["another_url1.jpg", "another_url2.jpg"]
        },
    ]

    try:
        asyncio.run(downloader.process_batch_async(batch_data))
        print("Batch processing completed.")
    except KeyboardInterrupt:
        print("\nCaught keyboard interrupt, shutting down...")
        asyncio.run(shutdown_async())
    except Exception as e:
        logger.error(f"An error occurred during batch processing: {e}\n{traceback.format_exc()}")