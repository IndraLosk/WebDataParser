class Reader:
    """
    File reader class
    Reads a text file and returns non-empty, stripped lines
    Raises:
        ValueError: If the file is empty or not found
    """

    def read_file(self, filename):
        """
        Read a file and return a list of non-empty lines
        Args:
            filename (str): Path to the file to read
        Return:
            list of string: Non-empty lines from the file
        Raises:
            ValueError: If the file is empty or does not exist
        """

        try:
            with open(filename, "r") as file:
                lines = [
                    line.strip() for line in file.readlines() if line.strip() != ""
                ]
                if len(lines) == 0:
                    raise ValueError("File is empty")
                return lines
        except FileNotFoundError:
            raise ValueError("File not found")
