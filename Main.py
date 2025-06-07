import sys
from Reader import Reader
from URLProcessing import *
from DownloadContent import *
from ProcessingDownloadContent import *
from FormingResultsRegistry import *


def main():
    if len(sys.argv) == 2:
        reader = Reader()
        try:
            urls = reader.read_file(sys.argv[1])
        except Exception as error:
            print(f"Error: {error}")

        try:
            formingResultsRegistry = FormingResultsRegistry()
            formingResultsRegistry.create_results_registry_csv()
            formingResultsRegistry.add_source_url(urls)

            urlProcessing = URLProcessing()
            new_urls = urlProcessing.cleaner(urls)

            urls_html, urls_pdf = urlProcessing.html_or_pdf(new_urls)
            formingResultsRegistry.add_processing_info_from_check()

            downloadContent = DownloadContent(urls_html, urls_pdf)
            downloadContent.download_files_request()
            # downloadContent.download_files_wget()

            downloadContent.download_html_request()
            # downloadContent.download_html_requestsHTMLsession()
            formingResultsRegistry.add_download_info()

            processingDownloadContent = ProcessingDownloadContent()
            processingDownloadContent.processing_pdf()
            processingDownloadContent.processing_html()

            formingResultsRegistry.add_processed_info()
            formingResultsRegistry.add_other()

            formingResultsRegistry.registry_sort()

        except Exception as error:
            print(f"Error: {error}")

    else:
        print("Error: Enter filename")


if __name__ == "__main__":
    main()
