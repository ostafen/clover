package clover

import (
	"github.com/ostafen/clover/internal"
)

type valueRange struct {
	start, end               interface{}
	includeStart, includeEnd bool
}

func (r1 *valueRange) intersect(r2 *valueRange) *valueRange {
	intersection := &valueRange{
		start:        r1.start,
		end:          r1.end,
		includeStart: r1.includeStart,
		includeEnd:   r1.includeEnd,
	}

	res := internal.Compare(r2.start, intersection.start)
	if res > 0 {
		intersection.start = r2.start
		intersection.includeStart = r2.includeStart
	} else if res == 0 {
		intersection.includeStart = intersection.includeStart && r2.includeStart
	} else if intersection.start == nil {
		intersection.start = r2.start
		intersection.includeStart = r2.includeStart
	}

	res = internal.Compare(r2.end, intersection.end)
	if res < 0 {
		intersection.end = r2.end
		intersection.includeEnd = r2.includeEnd
	} else if res == 0 {
		intersection.includeEnd = intersection.includeEnd && r2.includeEnd
	} else if intersection.end == nil {
		intersection.end = r2.end
		intersection.includeEnd = r2.includeEnd
	}
	return intersection
}
