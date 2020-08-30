from csv_reader.utils import read_previous_documents, flush_written_documents
import time
from os import listdir
from os.path import isfile, join


class InputWatcherService:
    def __init__(self, queue, input_directory, processed_files_file, periodic=0.01):
        self.queue = queue
        self.input_directory = input_directory
        self.processed_files_file = processed_files_file
        self.processed_files = read_previous_documents(processed_files_file)
        self.watcher_periodic = periodic

    def run(self):
        while True:
            files = {
                f
                for f in listdir(self.input_directory)
                if isfile(join(self.input_directory, f)) and f.endswith(".csv")
            }
            missing_files = files - self.processed_files
            if missing_files:
                for f in missing_files:
                    self.queue.put(join(self.input_directory, f))
            self.processed_files = self.processed_files & files
            time.sleep(self.watcher_periodic)

    def flush(self):
        with open(self.processed_files_file, "w+") as out_file:
            for f in self.processed_files:
                out_file.write(f + "\n")
