from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
import requests


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
        for url in urls:
            url_parsed = urlparse(url)
            if url_parsed.scheme == "https":
                query_params = parse_qs(url_parsed.query)
                query_params_temp = query_params.copy()
                for param in self.params_to_remove:
                    query_params.pop(param, None)

                if query_params_temp != query_params:
                    url = self.reassembly_url(url_parsed, query_params)

                if url not in cleaned_urls:
                    cleaned_urls.append(url)

        return cleaned_urls

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
        urls_html, urls_pdf = [], []
        header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        }
        for url in urls:
            try:
                response = requests.get(url, headers=header)
                if response.status_code == 200:
                    content_type = response.headers.get("Content-Type", "")
                    if "text/html" in content_type:
                        urls_html.append(url)
                    elif "application/pdf" in content_type:
                        urls_pdf.append(url)
            except Exception:
                # Ignore URLs that cause exceptions
                continue

        return urls_html, urls_pdf
