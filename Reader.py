class Reader:
    def read_file(self, filename):
        try:
            with open (filename, "r") as file:
                lines = [line.strip() for line in file.readlines() if line.strip() != '']
                return lines
        except FileNotFoundError:
            raise ValueError("File not found")
