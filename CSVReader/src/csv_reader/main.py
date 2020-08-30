from csv_reader.services import CSVReaderService, InputWatcherService, DataTransformationService
import yaml
import cerberus
from os.path import split, abspath, join
from queue import Queue
import threading
import signal
import sys
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

input_watcher_service = None

def cleanup():
    if input_watcher_service is not None:
        input_watcher_service.flush()

def signal_handler(signum, frame):
    signal.signal(signum, signal.SIG_IGN)
    cleanup()
    sys.exit(0)

def main():
    global input_watcher_service
    dirname, _ = split(abspath(__file__))
    with open(join(dirname, "schema.yaml")) as in_file:
        input_schema = yaml.load(in_file, Loader=Loader)
    with open(join(dirname, "output_schema.yaml")) as in_file:
        output_schema = yaml.load(in_file, Loader=Loader)
    with open(join(dirname, "..", "..", "settings.yaml")) as in_file:
        settings = yaml.load(in_file, Loader=Loader)
    
    settings["output_directory"] = join(dirname, settings["output_directory"])
    settings["error_directory"] = join(dirname, settings["error_directory"])
    settings["input_directory"] = join(dirname, settings["input_directory"])
    settings["processed_files"] = join(dirname, settings["processed_files"])

    queue = Queue()
    validator = cerberus.Validator(input_schema)
    transformation_service = DataTransformationService(output_schema)
    csv_reader_service = CSVReaderService(queue, settings["output_directory"], settings["error_directory"], validator, transformation_service)
    input_watcher_service = InputWatcherService(queue, settings["input_directory"], settings["processed_files"])

    watcher_thread = threading.Thread(target=input_watcher_service.run, daemon=True)
    reader_thread = threading.Thread(target=csv_reader_service.run, daemon=True)

    watcher_thread.start()
    reader_thread.start()

    watcher_thread.join()
    reader_thread.join()

if __name__ == "__main__":
    main()