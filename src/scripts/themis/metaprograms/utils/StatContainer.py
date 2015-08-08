class StatContainer(object):
    def __init__(self):
        self.data = []

    def __repr__(self):
        repr_dict = {}

        repr_dict["min"] = self.min()
        repr_dict["max"] = self.max()
        repr_dict["median"] = self.median()
        repr_dict["mean"] = self.mean()
        repr_dict["variance"] = self.variance()
        repr_dict["count"] = self.count()
        repr_dict["sum"] = self.sum()

        return "StatContainer " + str(repr_dict)

    def __eq__(self, other):
        if type(other) != StatContainer:
            return False
        return sorted(self.data) == sorted(other.data)

    def extend(self, val_list):
        self.data.extend(val_list)

    def append(self, val):
        self.data.append(val)

    def median(self):
        self.data.sort()

        if len(self.data) == 0:
            return None
        else:
            return self.data[len(self.data) / 2]

    def mean(self):
        if self.count() == 0:
            return None
        else:
            return sum(self.data) / len(self.data)

    def max(self):
        if self.count() > 0:
            return max(self.data)
        else:
            return None

    def min(self):
        if len(self.data) > 0:
            return min(self.data)
        else:
            return None

    def count(self):
        return len(self.data)

    def sum(self):
        return sum(self.data)

    def variance(self):
        if self.count() > 0:
            sum_squares = sum((x * x for x in self.data))

            return (sum_squares - (self.sum() * self.mean())) / self.count()
        else:
            return None

    def merge(self, other):
        self.data.extend(other.data)
