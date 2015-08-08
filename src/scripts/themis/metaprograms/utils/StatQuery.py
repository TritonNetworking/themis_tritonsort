import re

class StatQuery(object):
    def __init__(self, line_type_name, *args):
        self.field_filters = [
            {"field_name" : "line_type_name",
             "filter" : line_type_name
             }]

        self.description = None
        self.match_processor_function = None
        self.line_type_name = line_type_name

        for arg in args:
            field_filter = {}
            if len(arg) == 2:
                field_filter["field_name"] = arg[0]
                field_filter["filter"] = arg[1]

            self.field_filters.append(field_filter)

        self.regex = None

    def __repr__(self):
        return '<' + ', '.join(["%s = %s" % (f["field_name"], f["filter"])
                                for f in self.field_filters]) + '>'

    def get_regex(self):
        assert self.description is not None, (
            "Must have set a log line description corresponding to this "
            "query before using it for query processing; otherwise we can't "
            "associate field numbers with field names")

        if self.regex == None:
            self.regex = self._construct_regex(self.field_filters)

        return self.regex


    def _construct_regex(self, field_filters):
        num_field_filters = len(field_filters)
        if self.description.num_fields > num_field_filters:
            for i in xrange(self.description.num_fields - num_field_filters):
                field_index = num_field_filters + i
                field_filters.append(
                    {"field_name" : self.description.field_names[field_index],
                     "filter" : None})

        regex_string_pieces = []

        for field_filter in field_filters:
            curr_filter = field_filter["filter"]

            if curr_filter == None:
                regex_string_pieces.append("[^\s]+?")
            elif type(curr_filter) == list:
                regex_string_pieces.append("(?:%s)" % ('|'.join(curr_filter)))
            elif type(curr_filter) == str:
                regex_string_pieces.append("%s" % (curr_filter))
            elif type(curr_filter) == int:
                regex_string_pieces.append("%d" % (curr_filter))
            else:
                assert False, "Unknown filter type %s" % (type(curr_filter))

        regex = re.compile('^' + '\t'.join(regex_string_pieces) + '$')
        return regex

    @property
    def field_names(self):
        return [f["field_name"] for f in self.field_filters]

    def match(self, log_line):
        assert self.description is not None, (
            "Must have set a log line description corresponding to this "
            "query before using it for query processing; otherwise we can't "
            "associate field numbers with field names")

        if self.regex == None:
            self.regex = self._construct_regex(self.field_filters)

        match = self.regex.match(log_line)

        if match is None:
            return None

        # Translate the log line into a dictionary with appropriate types
        log_line_dict = self.description.translate(match.groups())

        # If we've gotten to this point, log line has cleared all filters
        # and can be emitted
        return log_line_dict

