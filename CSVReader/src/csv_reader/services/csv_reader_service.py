import csv
from collections import namedtuple
from copy import copy
from os.path import basename, join, splitext

from csv_reader.utils import (delete_file, read_document, write_error_file,
                              write_success_file)

Error = namedtuple("Error", "line errors")


class CSVReaderService:
    def __init__(
        self,
        queue,
        output_directory,
        error_directory,
        validator,
        transformation_service,
    ):
        self.queue = queue
        self.output_directory = output_directory
        self.error_directory = error_directory
        self.validator = validator
        self.transformation_service = transformation_service

    def run(self):
        while True:
            f = self.queue.get()
            self._process_file(f)

    def _process_file(self, f):
        csv_data = read_document(f)
        result_data = []
        error_data = []
        if csv_data:
            for i, row in enumerate(csv_data):
                errors = self._validate(row)
                if errors:
                    error_data.append(Error(i, errors))
                else:
                    transformed_data = self.transformation_service.transform(row)
                    result_data.append(transformed_data)
        print(f)
        self._handle_success(result_data, f)
        self._handle_error(error_data, f)

    def _validate(self, data):
        if not self.validator.validate(data):
            return self.validator.errors

    def _handle_error(self, errors, f):
        if errors:
            error_file_name = join(
                self.error_directory, splitext(basename(f))[0] + ".csv"
            )
            write_error_file(error_file_name, errors)

    def _handle_success(self, successes, f):
        if successes:
            success_file_name = join(
                self.output_directory, splitext(basename(f))[0] + ".json"
            )
            write_success_file(success_file_name, successes)
            # delete_file(f)
