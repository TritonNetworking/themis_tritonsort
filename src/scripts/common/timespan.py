#!/usr/bin/env python

class timespan(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end
                
        assert self.start <= self.end, "A timespan cannot stop before it "\
            "starts (%d, %d)" % (self.start, self.end)
            
    def __contains__(self, timespan):
        """
        A timespan "contains" another timespan if the containing timespan
        starts at or before the contained timespan does and ends at or after
        the contained timespan does
        """
        
        return self.start <= timespan.start and timespan.end <= self.end
    
    def __repr__(self):
        return "[%d, %d]" % (self.start, self.end)
    
    def __len__(self):
        return self.end - self.start
    
    def __eq__(self, other):
        return self.start == other.start and self.end == other.end
    
    def __cmp__(self, other):
        return cmp(self.start, other.start)
    
    def __sub__(self, otherSpan):
        if len(otherSpan) == 0:
            # If a span being subtracted takes no time, the resulting span
            # should be unaffected
            return [timespan(self.start, self.end)]
        
        if otherSpan == self:
            # Subtracting a span from itself should yield zero-length span
            # starting when this span starts
            return [timespan(self.start, self.start)]
            
        if self.end < otherSpan.start or self.start > otherSpan.end:
            # If this span finishes before the other span starts or starts
            # after the other span finishes, subtracting this span has no
            # effect.
            return [timespan(self.start, self.end)]
        
        if self.start >= otherSpan.start and self.end <= otherSpan.end:
            # Span being subtracted subsumes us, and so the result should be a
            # zero-length span
            return [timespan(self.start, self.start)]
        
        spans = []
        
        if self.start < otherSpan.start:
            # I start before the other span does
            spans.append(timespan(self.start, otherSpan.start))
        
        if otherSpan.end < self.end:
            # I end before the other span does
            spans.append(timespan(otherSpan.end, self.end))
            
        if len(spans) == 0:
            spans.append(timespan(self.start, self.end))
            
        return spans
    
    @staticmethod
    def makeTimespans(startTimes, endTimes):
        assert len(startTimes) == len(endTimes), "Lists passed to "\
            "makeTimespans must be the same length"
        
        return [timespan(startTimes[x], endTimes[x]) for x in \
                    xrange(len(startTimes))]
    
    
    
if __name__ == "__main__":
    span1 = timespan(2, 5)
    span2 = timespan(1, 9)
    emptySpan = timespan(4,4)
    
    # Testing __contains__
    assert span1 in span2
    assert span1 in span1
    assert span2 not in span1
    
    #Testing length
    assert len(emptySpan) == 0
    assert len(span1) == 3
    assert len(span2) == 8
    
    # Testing subtraction
    spans = span1 - span1
    assert len(spans) == 1
    assert len(spans[0]) == 0
    
    spans = span2 - span1
    assert spans == [timespan(1,2), timespan(5,9)]
    assert sum(map(len, spans)) + len(span1) == len(span2)
    
    """
    x1 [1   2   3]  4   5   6   7
    x2  1  [2   3   4   5]  6   7
    x3 [1   2]  3   4   5   6   7
    x4  1   2   3  [4   5]  6   7
    """
    
    x1 = timespan(1, 3)
    x2 = timespan(2, 5)
    x3 = timespan(1, 2)
    x4 = timespan(4, 5)
    
    assert x1 < x2
    assert x2 < x4
    
    assert x1 - x2 == [timespan(1,2)]
    assert x1 - x3 == [timespan(2,3)]
    assert x3 in x1
    assert x1 - x4 == [x1]
    assert x2 - x1 == [timespan(3,5)]
    assert x2 - x3 == [x2]
    assert x2 - x4 == [timespan(2,4)]
    assert x3 - x1 == [timespan(1,1)]
    assert x3 - x4 == [x3]
    assert x4 - x1 == [x4]
    assert x4 - x2 == [timespan(4,4)]
    assert x4 - x3 == [x4]
    
