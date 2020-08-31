VALID_INPUT = [
    {
        "INTERNAL_ID": "12345678",
        "FIRST_NAME": "Bobby",
        "MIDDLE_NAME": "",
        "LAST_NAME": "Tables",
        "PHONE_NUM": "555-555-5555",
    }
]
VALID_OUTPUT = {
    "id": "12345678",
    "name": {"first": "Bobby", "last": "Tables"},
    "phone": "555-555-5555",
}
