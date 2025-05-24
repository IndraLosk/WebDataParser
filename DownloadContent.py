import requests
import os
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import logging
from config import *
from requests_html import HTMLSession


class DownloadContent:
    """
    Class responsible for downloading HTML pages and PDF documents from given URLs.
    Attributes:
        urls_html (list): List of URLs HTML pages to download.
        urls_pdf (list): List of URLs PDF files to download.
    """

    def __init__(self, urls_html, urls_pdf):
        """
        Initializes the DownloadContent instance.
        Args:
            urls_html (list): List of URLs HTML pages to download.
            urls_pdf (list): List of URLs PDF files to download.
        Raises:
            ValueError: If either urls_html or urls_pdf is None.
        """
        logging.info("DownloadContent starts work")
        if urls_html is None or urls_pdf is None:
            logging.error("DownloadContent end work with, cause no arguments")
            raise ValueError("Enter arguments for downloading")
        self.urls_html = urls_html
        self.urls_pdf = urls_pdf

    def save_to_file(self, url, folder, index, mode):
        """
        Downloads content from a URL and saves it to a file.
        Args:
            url (str): The URL to download.
            folder (str): The folder path where to save the file.
            index (int): An index number to prefix.
            mode (str): File open mode - 'wb' for binary files (PDFs), 'w' for text files (HTML).
        """
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            logging.info("Url was get correct")
            url_parsed = urlparse(url)
            file_name = os.path.basename(url_parsed.path)
            file_name = unquote(file_name)
            if mode == "w" and (not file_name or "." not in file_name):
                file_name = file_name + "index.html"
            file_path = os.path.join(folder, f"{index}_{file_name}")

            with open(file_path, mode) as file:
                if mode == "wb":
                    file.write(response.content)
                else:
                    file.write(response.text)

            logging.info("File was saved correct")
        else:
            logging.warning(
                f"Non-200 status code {response.status_code} received for URL: {url}"
            )

    def download_one_file(self, folder, url, index):
        """
        Downloads a single PDF file.
        Args:
            folder (str): Folder to save the file.
            url (str): URL of the PDF file.
            index (int): Index to prefix the filename.
        Raises:
            ValueError: If download fails.
        """
        try:
            self.save_to_file(url, folder, index, "wb")
        except Exception as error:
            logging.warning(f"File wasn't saved. Error: {error}")
            raise ValueError(f"Error in downloading {url}: {error}")

    def download_files_request(self):
        """
        Downloads all PDF files in parallel using requests.
        """
        folder = "raw_downloads/documents/"
        os.makedirs(folder, exist_ok=True)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.download_one_file, folder, url, i): url
                for i, url in enumerate(self.urls_pdf)
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as error:
                    logging.warning(f"Download failed with error: {error}")
        logging.info("Files was downloaded correct")

    def download_files_wget(self):
        """
        Downloads all PDF files using wget.
        """
        with open("temp.txt", "w") as file:
            for url in self.urls_pdf:
                file.write(url + "\n")
        try:
            subprocess.run(
                [
                    "wget",
                    "-nd",
                    "-q",
                    "-i",
                    "temp.txt",
                    "-P",
                    "raw_downloads/documents/",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            logging.info("Every files was downloaded correct")
        except subprocess.CalledProcessError as error:
            logging.warning(f"Download failed with error: {error.stderr}")
            raise ValueError(f"Error in downloading files: {error}")
        finally:
            os.remove("temp.txt")

    def download_one_html(self, folder, url, index):
        """
        Downloads a single HTML page.

        Args:
            folder (str): Folder to save the file.
            url (str): URL of the HTML page.
            index (int): Index to prefix the filename.

        Raises:
            ValueError: If download fails.
        """
        try:
            self.save_to_file(url, folder, index, "w")
        except Exception as error:
            logging.warning(f"File wasn't saved. Error: {error}")
            raise ValueError(f"Error in downloading {url}: {error}")

    def download_html_request(self):
        """
        Downloads all HTML pages in parallel using requests.
        """
        folder = "raw_downloads/pages/"
        os.makedirs(folder, exist_ok=True)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.download_one_html, folder, url, i): url
                for i, url in enumerate(self.urls_html)
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as error:
                    logging.warning(f"Download failed with error: {error}")
        logging.info("Files was downloaded correct")

    def download_html_requestsHTMLsession(self):
        """
        Downloads all HTML pages using requests_html.
        """
        folder = "raw_downloads/pages"
        os.makedirs(folder, exist_ok=True)
        for i, url in enumerate(self.urls_html):
            session = HTMLSession()
            try:
                response = session.get(url, timeout=15)
                if response.status_code == 200:
                    response.html.render(timeout=15)
                    html_content = response.html.html

                    url_parsed = urlparse(url)
                    file_name = os.path.basename(url_parsed.path)
                    file_name = unquote(file_name)
                    if not file_name or "." not in file_name:
                        file_name = "index.html"
                    file_path = os.path.join(folder, f"{i}_{file_name}")

                    with open(file_path, "w") as file:
                        file.write(html_content)

                    logging.info("File was saved correct")
                else:
                    logging.warning(
                        f"Non-200 status code {response.status_code} received for URL: {url}"
                    )
            except Exception as error:
                logging.warning(f"File wasn't saved. Error: {url}: {error}")
