import json

class LogLineDescription(object):
    def __init__(self, description_json):
        self.parse_functions = []

        fields = description_json["fields"]

        self.num_fields = len(fields)

        self.field_names = []
        self.field_number_to_name = {}

        for index, field in enumerate(fields):
            name = field["name"]
            type_str = field["type"]

            self.field_number_to_name[index] = name
            self.field_names.append(name)

            if type_str in ["int", "uint"]:
                self.parse_functions.append(int)
            elif type_str == "str":
                self.parse_functions.append(str)
            else:
                sys.exit("Unknown field type '%s' encountered" % (type_str))

        # Log line typename is always in the first position
        self.line_type_name = description_json["type"]

    def __repr__(self):
        return ("Description(" +  self.line_type_name + ', ' +
                ', '.join(self.field_names[1:]) + ")")

    def log_line_to_dict(self, log_line):
        """
        Given the regex match groups from a log line that matches this
        description, return a dictionary mapping field names to
        appropriately-typed values
        """
        log_dict = {}
        for (i, chunk) in enumerate(log_line):
            log_dict[self.field_number_to_name[i]] = \
                self.parse_functions[i](chunk)

        return log_dict

    def matches_query(self, query):
        query_field_names = query.field_names

        return ((self.field_names[:len(query_field_names)] == query_field_names)
                and (self.line_type_name == query.line_type_name))


def load_descriptions_from_file(filename):
    descriptions = []

    with open(filename, 'r') as fp:
        description_list_json = json.load(fp)

        for description_json in description_list_json:
            descriptions.append(LogLineDescription(description_json))

    return descriptions
