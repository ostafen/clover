package clover

import (
	"github.com/ostafen/clover/internal"
)

type valueRange struct {
	start, end                 interface{}
	startIncluded, endIncluded bool
}

func (r *valueRange) isNil() bool {
	return r.start == nil && r.end == nil && r.startIncluded && r.endIncluded
}

func (r1 *valueRange) intersect(r2 *valueRange) *valueRange {
	intersection := &valueRange{
		start:         r1.start,
		end:           r1.end,
		startIncluded: r1.startIncluded,
		endIncluded:   r1.endIncluded,
	}

	res := internal.Compare(r2.start, intersection.start)
	if res > 0 {
		intersection.start = r2.start
		intersection.startIncluded = r2.startIncluded
	} else if res == 0 {
		intersection.startIncluded = intersection.startIncluded && r2.startIncluded
	} else if intersection.start == nil {
		intersection.start = r2.start
		intersection.startIncluded = r2.startIncluded
	}

	res = internal.Compare(r2.end, intersection.end)
	if res < 0 {
		intersection.end = r2.end
		intersection.endIncluded = r2.endIncluded
	} else if res == 0 {
		intersection.endIncluded = intersection.endIncluded && r2.endIncluded
	} else if intersection.end == nil {
		intersection.end = r2.end
		intersection.endIncluded = r2.endIncluded
	}
	return intersection
}

func unaryCriteriaToRange(c *UnaryCriteria) *valueRange {
	switch c.OpType {
	case EqOp:
		return &valueRange{
			start:         c.Value,
			end:           c.Value,
			startIncluded: true,
			endIncluded:   true,
		}
	case LtOp:
		return &valueRange{
			start:         nil,
			end:           c.Value,
			startIncluded: false,
			endIncluded:   false,
		}
	case LtEqOp:
		return &valueRange{
			start:         nil,
			end:           c.Value,
			startIncluded: false,
			endIncluded:   true,
		}
	case GtOp:
		return &valueRange{
			start:         c.Value,
			end:           nil,
			startIncluded: false,
			endIncluded:   false,
		}
	case GtEqOp:
		return &valueRange{
			start:         c.Value,
			end:           nil,
			startIncluded: true,
			endIncluded:   false,
		}
	}
	return nil
}
