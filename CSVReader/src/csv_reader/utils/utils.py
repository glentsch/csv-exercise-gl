import csv
import json
from os import remove
from os.path import exists

OUTPUT_COL_LINE_NO = "LINE_NO"
OUTPUT_COL_ERROR_MSG = "ERROR_MSG"


def read_previous_documents(f):
    data = set()
    if not exists(f):
        return data
    with open(f) as in_file:
        for line in in_file:
            line_data = line.replace("\n", "")
            if line_data:
                data.add(line_data)
    return data


def flush_written_documents(f, processed_files):
    with open(f, "w") as out_file:
        for data in processed_files:
            out_file.write(data + "\n")


def read_document(f):
    with open(f) as in_file:
        csv_reader = csv.DictReader(in_file)
        return list(csv_reader)


def write_error_file(f, errors):
    with open(f, "w+") as out_file:
        writer = csv.DictWriter(
            out_file, fieldnames=(OUTPUT_COL_LINE_NO, OUTPUT_COL_ERROR_MSG)
        )
        writer.writeheader()
        for error in errors:
            writer.writerow(
                {
                    OUTPUT_COL_LINE_NO: error.line,
                    OUTPUT_COL_ERROR_MSG: json.dumps(error.errors),
                }
            )


def write_success_file(f, successes):
    with open(f, "w+") as out_file:
        json.dump(successes, out_file)


def delete_file(f):
    remove(f)
