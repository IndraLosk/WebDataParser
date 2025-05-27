from PyPDF2 import PdfReader
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import *
from bs4 import BeautifulSoup


class ProcessingDownloadContent:
    """
    Class for processing PDF and HTML files: extracts text content
    and saves it to specified folders.
    """

    def processing_one_pdf(self, file_path, folder):
        """
        Processes a single PDF file: extracts text from all pages and saves it as a TXT file.
        Args:
            file_path (str): Path to the source PDF file
            folder (str): Folder to save the output TXT file
        Raises:
            ValueError: If an error occurs during file processing
        """
        try:
            reader = PdfReader(file_path)
            output_path = os.path.join(folder, os.path.basename(file_path)) + ".txt"

            with open(output_path, "a") as file:
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        file.write(text)
            logging.info(f"Successfully processed PDF")
        except Exception as error:
            logging.warning(f"Error processing {file_path}: {error}")
            raise ValueError(f"Error processing: {error}")

    def processing_pdf(self):
        """
        Method to process all PDF files in the "raw_downloads/documents/" folder.
        """
        folder = "processed_data/documents/"
        os.makedirs(folder, exist_ok=True)

        files_paths = []

        for file_name in os.listdir("raw_downloads/documents/"):
            file_path = os.path.join("raw_downloads/documents/", file_name)
            if os.path.isfile(file_path):
                files_paths.append(file_path)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.processing_one_pdf, file_path, folder): file_path
                for file_path in files_paths
            }
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    future.result()
                except Exception as error:
                    logging.warning(
                        f"Processing failed for {file_path} with error: {error}"
                    )
        logging.info("Files was processed correct")

    def processing_one_html(self, file_path, folder):
        """
        Processes a single HTML file: removes scripts/styles, extracts text, and saves as TXT.
        Args:
            file_path (str): Path to the source HTML file
            folder (str): Folder to save the output TXT file
        Raises:
            ValueError: If an error occurs during file processing
        """
        try:
            with open(file_path, "r") as file:
                html_content = file.read()

            soup = BeautifulSoup(html_content, "html.parser")
            for script_or_style in soup(["script", "style"]):
                script_or_style.decompose()
            text = soup.get_text()
            output_path = os.path.join(folder, os.path.basename(file_path)) + ".txt"

            with open(output_path, "a") as file:
                file.write(text)
            logging.info(f"Successfully processed HTML")
        except Exception as error:
            logging.warning(f"Error processing {file_path}: {error}")
            raise ValueError(f"Error processing: {error}")

    def processing_html(self):
        """
        Method to process all HTML files in the "raw_downloads/pages/" folder.
        """
        folder = "processed_data/pages/"
        os.makedirs(folder, exist_ok=True)

        files_paths = []

        for file_name in os.listdir("raw_downloads/pages/"):
            file_path = os.path.join("raw_downloads/pages/", file_name)
            if os.path.isfile(file_path):
                files_paths.append(file_path)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.processing_one_html, file_path, folder): file_path
                for file_path in files_paths
            }
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    future.result()
                except Exception as error:
                    logging.warning(
                        f"Processing failed for {file_path} with error: {error}"
                    )
        logging.info("Files was processed correct")
