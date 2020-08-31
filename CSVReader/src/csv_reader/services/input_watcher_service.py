import time
from os import listdir
from os.path import isfile, join

from csv_reader.utils import flush_written_documents, read_previous_documents


class InputWatcherService:
    def __init__(self, queue, input_directory, processed_files_file, periodic=0.01):
        self.queue = queue
        self.input_directory = input_directory
        self.processed_files_file = processed_files_file
        self.processed_files = read_previous_documents(processed_files_file)
        self.watcher_periodic = periodic
    def _get_files(self):
        files = {
                f
                for f in listdir(self.input_directory)
                if isfile(join(self.input_directory, f)) and f.endswith(".csv")
            }
        missing_files = files - self.processed_files
        if missing_files:
            for f in missing_files:
                self.queue.put(join(self.input_directory, f))
        self.processed_files |= files

    def run(self):
        while True:
            self._get_files()
            time.sleep(self.watcher_periodic)

    def flush(self):
        with open(self.processed_files_file, "w+") as out_file:
            for f in self.processed_files:
                out_file.write(f + "\n")
