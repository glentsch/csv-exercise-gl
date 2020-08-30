class DataTransformationService:
    def __init__(self, output_schema):
        self.schema = output_schema

    def _to_schema(self, data):
        result_data = {}
        for k, v in data.items():
            definition = self.schema.get(k)
            if definition:
                to = definition["to"]
                tos = to.split(".")
                ref = result_data
                for single_ref in tos[:-1]:
                    if ref.get(single_ref) is None:
                        ref[single_ref] = {}
                    ref = ref[single_ref]
                print(f"{k} {definition.get('omitifempty')} {v}")
                if not (definition.get("omitifempty") and not v):
                    ref[tos[-1]] = v
        return result_data

    def transform(self, data):
        return self._to_schema(data)
