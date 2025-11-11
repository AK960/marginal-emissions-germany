import chardet


class Helpers:
    def __init__(self, path):
        self.path=path

    def check_encoding(self, path):
        with open(path, 'rb') as f:
            raw = f.read(10000) # first 10 KB for encoding detection
            encoding = chardet.detect(raw)['encoding']

        return encoding['encoding'].to


    def say_hello(self):
        print("Hello from marginal_emissions!")

