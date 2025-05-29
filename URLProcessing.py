from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from config import *
from FormingResultsRegistry import *
from datetime import datetime


class URLProcessing:
    """
    Class for URL processing and detecting their content types.
    Attributes:
        params_to_remove (list): List of query parameters to remove from URLs.
        It can be set, but by default it clears from utm_source, fbclid, etc.

    """

    def __init__(self, params_to_remove=None):
        """
        Initializes the URLProcessing instance.
        Args:
            params_to_remove (list): List of query parameters to remove from URLs.
        """
        logging.info("URLProcessing starts work")
        if params_to_remove == None:
            self.params_to_remove = [
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
                "fbclid",
                "gclid",
                "ysclid",
            ]
        else:
            self.params_to_remove = params_to_remove

        self.registry = FormingResultsRegistry()

    def reassembly_url(self, url_parsed, query_params):
        """
        Reassembly url for updated query parameters.
        Args:
            url_parsed (ParseResult): Parsed URL object from urlparse.
            query_params (dict): Dictionary of query parameters to include in the URL.
        Return:
            str: The reconstructed URL.
        """
        new_query = urlencode(query_params, doseq=True)
        new_url = urlunparse(
            (
                url_parsed.scheme,
                url_parsed.netloc,
                url_parsed.path,
                url_parsed.params,
                new_query,
                url_parsed.fragment,
            )
        )
        logging.info(f"URL was reassembly, new URL: {new_url}")
        return new_url

    def cleaner(self, urls):
        """
        Cleans a list of URLs by removing unwanted query parameters.
        Args:
            urls (list): List of URL strings to clean.
        Return:
            list of string: List of cleaned URLs.
        """

        cleaned_urls = []
        for id, url in enumerate(urls, start=0):
            url_parsed = urlparse(url)
            if url_parsed.scheme == "https" or url_parsed.scheme == "http":
                query_params = parse_qs(url_parsed.query)
                query_params_temp = query_params.copy()
                for param in self.params_to_remove:
                    query_params.pop(param, None)

                if query_params_temp != query_params:
                    logging.info(f"query_params was changed for {url}")
                    url = self.reassembly_url(url_parsed, query_params)

                if url not in cleaned_urls:
                    cleaned_urls.append(url)
                    download_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.registry.add_processing_info_from_cleaner(
                        id, url, download_timestamp, "clean_url"
                    )
                else:
                    download_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.registry.add_processing_info_from_cleaner(
                        id, url, download_timestamp, "duplicate_url"
                    )
            else:
                download_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logging.info(f"Line {url} isn't URL")
                self.registry.add_processing_info_from_cleaner(
                    id, url, download_timestamp, "Not url"
                )

        logging.info("Every URL was cleaned")
        return cleaned_urls

    def check_html_or_pdf(self, id, url, header):
        """
        Determines the content type of a URL.
        Args:
            url (str): The URL to check.
            header (dict): HTTP headers to send with the request.
        Returns:
        tuple: A tuple containing:
            - url (str): The URL that was checked.
            - return_url_type (str): The content type of the URL, which can be 'html', 'pdf', or an empty string if unknown.
        """
        return_url_type = ""
        try:
            response = requests.head(url, headers=header, timeout=15)
            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    return_url_type = "html"
                    logging.info(f"{id} URL {url} is html")
                elif "application/pdf" in content_type:
                    return_url_type = "pdf"
                    logging.info(f"{id} URL {url} is PDF")
            else:
                logging.warning(
                    f"{id} URL {url}.Error: Non-200 status code {response.status_code}"
                )
        except Exception as error:
            logging.warning(
                f"{id} URL {url} can't be checked html or pdf. Error: {error}"
            )
            # Ignore URLs that cause exceptions
            pass

        logging.info("URL type was been determined")
        return url, return_url_type

    def html_or_pdf(self, urls):
        """
        Sorts URLs into HTML and PDF categories based on their Content-Type.
        Args:
            urls (list): List of URL strings to classify.
        Return:
            tuple: Two lists containing URLs with HTML and PDF content types.
                - urls_html (list of str): URLs with 'text/html' content type.
                - urls_pdf (list of str): URLs with 'application/pdf' content type.
        """
        header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        }
        urls_html, urls_pdf = [], []
        logging.info("Start checking pdf or html")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.check_html_or_pdf, i, url, header): (i, url)
                for i, url in enumerate(urls)
            }
            for future in as_completed(futures):
                url, url_type = future.result()
                if url_type == "html":
                    urls_html.append(url)
                elif url_type == "pdf":
                    urls_pdf.append(url)

        logging.info("URLs types were been determined")
        return urls_html, urls_pdf
