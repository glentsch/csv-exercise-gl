from unittest import TestCase

from csv_reader.services.data_transformation_service import \
    DataTransformationService
from tests.unit.fixtures import VALID_INPUT, VALID_OUTPUT


class DataTransformationServiceTest(TestCase):
    def setUp(self):
        self.output_schema = {
            "INTERNAL_ID": {"to": "id"},
            "FIRST_NAME": {"to": "name.first"},
            "MIDDLE_NAME": {"to": "name.middle", "omitifempty": True},
            "LAST_NAME": {"to": "name.last"},
            "PHONE_NUM": {"to": "phone"},
        }
        self.test_class = DataTransformationService(self.output_schema)

    def test_transform(self):
        actual = self.test_class.transform(VALID_INPUT[0])

        self.assertDictEqual(VALID_OUTPUT, actual)
