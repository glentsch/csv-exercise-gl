from unittest import TestCase
from unittest.mock import Mock, patch

from csv_reader.services.csv_reader_service import CSVReaderService, Error
from tests.unit.fixtures import VALID_INPUT, VALID_OUTPUT


class CSVReaderServiceTest(TestCase):
    def setUp(self):
        self.validator = Mock()
        self.queue = Mock()
        self.output_directory = ""
        self.error_directory = ""
        self.transformation_service = Mock()
        self.test_class = CSVReaderService(
            self.queue,
            self.output_directory,
            self.error_directory,
            self.validator,
            self.transformation_service,
        )

    @patch("csv_reader.services.csv_reader_service.delete_file")
    @patch("csv_reader.services.csv_reader_service.read_document")
    @patch("csv_reader.services.csv_reader_service.write_success_file")
    def test__process_file_valid(
        self, patched_write_success_file, patched_read_document, patched_delete_file
    ):
        patched_read_document.return_value = VALID_INPUT
        self.validator.validate.return_value = True
        self.transformation_service.transform.return_value = VALID_OUTPUT
        f = "test_file.csv"

        self.test_class._process_file(f)

        self.validator.validate.assert_called_once_with(VALID_INPUT[0])
        self.transformation_service.transform.assert_called_once_with(VALID_INPUT[0])
        patched_read_document.assert_called_once_with(f)
        patched_write_success_file.assert_called_once_with(
            "test_file.json", [VALID_OUTPUT]
        )
        patched_delete_file.assert_called_once_with(f)

    @patch("csv_reader.services.csv_reader_service.delete_file")
    @patch("csv_reader.services.csv_reader_service.read_document")
    @patch("csv_reader.services.csv_reader_service.write_error_file")
    def test__process_file_on_error(
        self, patched_write_error_file, patched_read_document, patched_delete_file
    ):
        patched_read_document.return_value = VALID_INPUT
        self.validator.validate.return_value = False
        self.validator.errors = [{"field": "invalid"}]
        f = "test_file.csv"

        self.test_class._process_file(f)

        self.validator.validate.assert_called_once_with(VALID_INPUT[0])
        patched_read_document.assert_called_once_with(f)
        patched_write_error_file.assert_called_once_with(
            f, [Error(0, [{"field": "invalid"}])]
        )
