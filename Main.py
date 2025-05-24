import sys
from Reader import Reader
from URLProcessing import *
from DownloadContent import *
from concurrent.futures import ThreadPoolExecutor


def main():
    """
    Main function that orchestrates the workflow:
    1. Reads URLs from an input file.
    2. Cleans and classifies URLs.
    3. Downloads content (PDFs and HTML pages).
    """
    if len(sys.argv) == 2:
        reader = Reader()
        try:
            urls = reader.read_file(sys.argv[1])
        except Exception as error:
            print(f"Error: {error}")

        try:
            urlProcessing = URLProcessing()
            new_urls = urlProcessing.cleaner(urls)
            urls_html, urls_pdf = urlProcessing.html_or_pdf(new_urls)

            downloadContent = DownloadContent(urls_html, urls_pdf)
            downloadContent.download_files_request()
            downloadContent.download_files_wget()

            downloadContent.download_html_request()
            downloadContent.download_html_requestsHTMLsession()

        except Exception as error:
            print(f"Error: {error}")

    else:
        print("Error: Enter filename")


if __name__ == "__main__":
    main()
