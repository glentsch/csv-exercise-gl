from unittest import TestCase
from unittest.mock import Mock, patch

from csv_reader.services.input_watcher_service import InputWatcherService


class InputWatcherServiceTest(TestCase):
    @patch("csv_reader.services.input_watcher_service.read_previous_documents")
    def setUp(self, patched_read_previous_documents):
        patched_read_previous_documents.return_value = {"file1.csv"}
        self.queue = Mock()
        self.input_directory = "input/"
        self.processed_files_file = "processed_files.csv"
        self.test_class = InputWatcherService(
            self.queue, self.input_directory, self.processed_files_file
        )

    @patch("csv_reader.services.input_watcher_service.listdir")
    @patch("csv_reader.services.input_watcher_service.isfile")
    def test__get_files(self, patched_is_file, patched_list_dir):
        patched_list_dir.return_value = ["file1.csv", "file2.csv", "otherfile"]
        patched_is_file.return_value = True

        self.test_class._get_files()

        self.queue.put.assert_called_once_with("input/file2.csv")

        self.assertEqual({"file1.csv", "file2.csv"}, self.test_class.processed_files)
