import json
import jsonschema2md2

parser = jsonschema2md2.Parser(
    examples_as_yaml=True,
    show_examples="all",
)
with open("../examples/ksml.json", "r") as json_file:
    md_lines = parser.parse_schema(json.load(json_file))
print(''.join(md_lines))
