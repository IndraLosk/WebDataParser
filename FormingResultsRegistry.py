import os
import subprocess


class FormingResultsRegistry:
    """
    A class that generates a summary register based on the results of processing all URLs from the input CSV file
    """

    def create_results_registry_csv(self):
        """
        Creates a new results_registry.csv file with the appropriate columns.
        This method initializes the registry with all necessary fields for tracking document processing.

        Columns:
        ----------
        id : int
            Unique sequential number or record identifier.
        source_url : str
            URL from the input CSV file.
        final_url : str
            URL from which the content was actually downloaded (after redirects).
        processing_timestamp: str
            Date and time of processing in the format YYYY-MM-DD HH:MM:SS.
        download_timestamp : str
            Date and time of download in the format YYYY-MM-DD HH:MM:SS.
        download_status : str
            Status of download: 'success', 'failed_download', 'failed_processing', 'skipped_robots'.
        error_message : str
            Brief description of any error encountered.
        content_type_detected : str
            Detected content type: 'document' or 'page'.
        raw_file_path : str
            Relative path to the saved raw file or page.
        processed_file_path : str
            Relative path to the file with cleaned text.
        file_size_bytes : int
            Size of the raw file in bytes, if applicable.
        document_page_count : int
            Number of pages if the file is a document and page count was determined.
        detected_language : str
            Detected language of the document or page.
        extracted_keywords : str
            Extracted keywords separated by commas, if applicable.
        extracted_entities : str, optional
            Extracted named entities, if implemented.
        summary : str, optional
            Brief summary of the document, if implemented.
        metadata_author : str, optional
            Author from document metadata, if available.
        metadata_creation_date : str, optional
            Creation date from document metadata, if available.
        """
        columns = [
            "id",
            "source_url",
            "final_url",
            "processing_timestamp",
            "download_timestamp",
            "download_status",
            "error_message",
            "content_type_detected",
            "raw_file_path",
            "processed_file_path",
            "file_size_bytes",
            "document_page_count",
            "detected_language",
            "extracted_keywords",
            "extracted_entities",
            "summary",
            "metadata_author",
            "metadata_creation_date",
        ]

        with open("results_registry.csv", "w") as file:
            first_line = ",".join(columns) + "\n"
            file.write(first_line)

    def add_source_url(self, source_urls):
        """
        Appends source URLs to the results_registry.csv file.
        Args:
            source_urls (list): A list of source URLs to be added to the registry.
        """
        with open("results_registry.csv", "a") as file:
            for id in range(1, len(source_urls) + 1):
                line = f"{id},{source_urls[id - 1]}{", "*17}\n"
                file.write(line)

    def add_processing_info_from_cleaner(self, id, url, status):
        """
        Updates the registry with processing information from the URL cleaning step.
        Args:
            id (int): The ID of the entry to update.
            url (str): The cleaned or processed URL.
            status (str): The status of the URL after cleaning.
        Notes:
            - Updates 'final_url' (column 2) with the cleaned URL.
            - Updates 'error_message' (column 6) if the URL is a duplicate or invalid.
        """

        id += 1
        with open("results_registry.csv", "r") as inputfile, open(
            "results_registry_temp.csv", "a"
        ) as outputfile:
            outputfile.write(next(inputfile))
            for line in inputfile:
                temp_line = line.rstrip("\n")
                columns = temp_line.split(",")
                if columns[0] == str(id):
                    if status == "clean_url":
                        columns[2] = url
                    if status == "duplicate_url":
                        columns[2] = "-"
                        columns[6] = "URL was deleted cause URL is duplicate"
                    if status == "Not url":
                        columns[2] = "-"
                        columns[6] = "Line was deleted cause line isn't URL"

                new_line = ",".join(columns) + "\n"
                outputfile.write(new_line)

        os.replace("results_registry_temp.csv", "results_registry.csv")

    def get_ids(self):
        """
        Retrieves all IDs from the results_registry.csv file.
        Returns:
            set: A set containing all IDs present in the registry.
        """
        ids = set()
        with open("results_registry.csv", "r") as file:
            next(file)
            for line in file:
                temp_line = line.split(",")
                ids.add(temp_line[0])

        return ids

    def get_ids_without_error(self):
        """
        Retrieves IDs from the registry that do not have an error message.
        Returns:
            set: A set of IDs with no error messages.
        """
        ids = set()
        with open("results_registry.csv", "r") as file:
            next(file)
            for line in file:
                temp_line = line.split(",")
                if len(temp_line[5]) == 1:
                    ids.add(temp_line[0])

        return ids

    def add_processing_info_from_check(self):
        """
        Updates the registry with information from the document type and error checking step based on analytics.log.
        Notes:
            - Updates 'error_message' (column 6) if an error is detected during document type or error checking.
            - Updates 'content_type_detected' (column 7) with the detected content type ('document' or 'page').
        """
        ids = self.get_ids()
        with open("analytics.log", "r") as logfile, open(
            "results_registry.csv", "r"
        ) as inputfile, open("results_registry_temp.csv", "a") as outputfile:
            outputfile.write(next(inputfile))
            is_read = False
            for log_line in logfile:
                if is_read:
                    inputfile.seek(0)
                    next(inputfile)
                    for in_line in inputfile:
                        temp_line = in_line.rstrip("\n")
                        columns = temp_line.split(",")
                        log_line_split = log_line.split(" ")
                        if columns[2] == log_line_split[4] and len(columns[6]) == 1:
                            if columns[0] in ids:
                                ids.remove(columns[0])
                            if "Error:" in log_line_split:
                                error_index = log_line_split.index("Error:")
                                error_text = " ".join(
                                    log_line_split[error_index + 1 :]
                                ).strip()
                                columns[6] = f'"{error_text}"'
                            else:
                                columns[7] = log_line_split[6].rstrip("\n")
                            new_line = ",".join(columns) + "\n"
                            outputfile.write(new_line)
                if "Start checking pdf or html" in log_line:
                    is_read = True

            for id_to_find in list(ids):
                inputfile.seek(0)
                for line in inputfile:
                    columns = line.rstrip("\n").split(",")
                    if columns[0] == id_to_find:
                        columns[7] = "-"
                        new_line = ",".join(columns) + "\n"
                        outputfile.write(new_line)
                        ids.remove(id_to_find)

        os.replace("results_registry_temp.csv", "results_registry.csv")

    def add_download_info(self):
        """
        Updates the registry with download information from analytics.log.
        Notes:
            - Updates 'download_timestamp' (column 4) with the date and time of download.
            - Updates 'download_status' (column 5) with the result of the download operation ('Successful download' or '-').
            - Updates 'error_message' (column 6) if an error occurred during download.
            - Updates 'raw_file_path' (column 8) with the relative path to the downloaded file.
            - Updates 'file_size_bytes' (column 10) with the size of the downloaded file in bytes.
        """

        ids = self.get_ids()
        with open("analytics.log", "r") as logfile, open(
            "results_registry.csv", "r"
        ) as inputfile, open("results_registry_temp.csv", "a") as outputfile:
            outputfile.write(next(inputfile))
            is_read = False
            for log_line in logfile:
                if is_read:
                    inputfile.seek(0)
                    next(inputfile)
                    for in_line in inputfile:
                        temp_line = in_line.rstrip("\n")
                        columns = temp_line.split(",")
                        log_line_split = log_line.split(" ")
                        if columns[2] == log_line_split[4].rstrip("."):
                            if columns[0] in ids:
                                ids.remove(columns[0])
                            if "Error:" in log_line_split:
                                columns[4] = columns[5] = columns[8] = columns[10] = "-"
                                error_index = log_line_split.index("Error:")
                                error_text = "".join(
                                    log_line_split[error_index + 1 :]
                                ).strip()
                                columns[6] = f'"{error_text}"'
                            else:
                                columns[4] = (
                                    log_line_split[0] + " " + log_line_split[1][:-4]
                                )
                                columns[10] = log_line_split[7]
                                columns[8] = log_line_split[11].rstrip(".")
                                columns[5] = "Successful download"
                            new_line = ",".join(columns) + "\n"
                            outputfile.write(new_line)
                if "Start domload PDF" in log_line:
                    is_read = True

            for id_to_find in list(ids):
                inputfile.seek(0)
                for line in inputfile:
                    columns = line.rstrip("\n").split(",")
                    if columns[0] == id_to_find:
                        columns[4] = columns[5] = columns[8] = columns[10] = "-"
                        new_line = ",".join(columns) + "\n"
                        outputfile.write(new_line)
                        ids.remove(id_to_find)

        os.replace("results_registry_temp.csv", "results_registry.csv")

    def add_processed_info(self):
        """
        Updates the registry with information about processed files from analytics.log.
        Notes:
            - Updates 'processing_timestamp' (column 3) with the date and time of processing.
            - Updates 'processed_file_path' (column 9) with the relative path to the processed file.
            - Updates 'document_page_count' (column 11) with the number of pages, if applicable.
            - Updates 'detected_language' (column 12) with the detected language or 'Not detected'.
            - Updates 'error_message' (column 6) if an error occurred during processing.
        """
        ids = self.get_ids()
        with open("analytics.log", "r") as logfile, open(
            "results_registry.csv", "r"
        ) as inputfile, open("results_registry_temp.csv", "a") as outputfile:
            outputfile.write(next(inputfile))
            is_read = False
            for log_line in logfile:
                if is_read:
                    inputfile.seek(0)
                    next(inputfile)
                    for in_line in inputfile:
                        temp_line = in_line.rstrip("\n")
                        columns = temp_line.split(",")
                        log_line_split = log_line.split(" ")
                        if columns[8] == log_line_split[4]:
                            if columns[0] in ids:
                                ids.remove(columns[0])
                            if "Error:" in log_line_split:
                                columns[3] = columns[9] = columns[11] = columns[12] = (
                                    "-"
                                )
                                error_index = log_line_split.index("Error")
                                error_text = "".join(
                                    log_line_split[error_index + 1 :]
                                ).strip()
                                columns[6] = f'"{error_text}"'
                            else:
                                columns[3] = (
                                    log_line_split[0] + " " + log_line_split[1][:-4]
                                )
                                columns[9] = log_line_split[10]
                                if len(log_line_split) > 15:
                                    columns[11] = log_line_split[15]
                                else:
                                    columns[11] = "-"
                                if "None" not in log_line_split[13]:
                                    columns[12] = (
                                        log_line_split[13].rstrip(".").rstrip("\n")
                                    )
                                else:
                                    columns[12] = "Not detected"
                            new_line = ",".join(columns) + "\n"
                            outputfile.write(new_line)
                if "Start processing PDF" in log_line:
                    is_read = True

            for id_to_find in list(ids):
                inputfile.seek(0)
                for line in inputfile:
                    columns = line.rstrip("\n").split(",")
                    if columns[0] == id_to_find:
                        columns[3] = columns[9] = columns[11] = columns[12] = "-"
                        new_line = ",".join(columns) + "\n"
                        outputfile.write(new_line)
                        ids.remove(id_to_find)

        os.replace("results_registry_temp.csv", "results_registry.csv")

    def add_other(self):
        """
        Fills the remaining columns in the registry with placeholder values.
        Notes:
            - Sets 'extracted_keywords' (column 13) to '-'.
            - Sets 'extracted_entities' (column 14) to '-'.
            - Sets 'summary' (column 15) to '-'.
            - Sets 'metadata_author' (column 16) to '-'.
            - Sets 'metadata_creation_date' (column 17) to '-'.
            - Used when no extraction or metadata is available.
        """
        with open("results_registry.csv", "r") as inputfile, open(
            "results_registry_temp.csv", "a"
        ) as outputfile:
            for line in inputfile:
                columns = line.rstrip("\n").split(",")
                columns[13] = columns[14] = columns[15] = columns[16] = "-"
                new_line = ",".join(columns) + "\n"
                outputfile.write(new_line)

        os.replace("results_registry_temp.csv", "results_registry.csv")

    def registry_sort(self):
        """
        Sorts the 'results_registry.csv' file by the 'id' column.
        """
        input_file = "results_registry.csv"
        temp_file = "results_registry_temp.csv"
        cmd = f"(head -n 1 {input_file} && tail -n +2 {input_file} | sort -t, -k1,1n) > {temp_file}"

        subprocess.run(cmd, shell=True, check=True)
        os.replace(temp_file, input_file)
