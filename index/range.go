package index

import (
	"github.com/ostafen/clover/v2/internal"
)

type Range struct {
	Start, End                 interface{}
	StartIncluded, EndIncluded bool
}

func (r *Range) IsEmpty() bool {
	if (r.Start == nil && !r.StartIncluded && r.End != nil) || (r.End == nil && !r.EndIncluded && r.Start != nil) {
		return false
	}

	res := internal.Compare(r.Start, r.End)
	return (res > 0) || (res == 0 && !r.StartIncluded && !r.EndIncluded)
}

func (r *Range) IsNil() bool {
	return r.Start == nil && r.End == nil && r.StartIncluded && r.EndIncluded
}

func (r *Range) Intersect(r2 *Range) *Range {
	intersection := &Range{
		Start:         r.Start,
		End:           r.End,
		StartIncluded: r.StartIncluded,
		EndIncluded:   r.EndIncluded,
	}

	res := internal.Compare(r2.Start, intersection.Start)
	if res > 0 {
		intersection.Start = r2.Start
		intersection.StartIncluded = r2.StartIncluded
	} else if res == 0 {
		intersection.StartIncluded = intersection.StartIncluded && r2.StartIncluded
	} else if intersection.Start == nil {
		intersection.Start = r2.Start
		intersection.StartIncluded = r2.StartIncluded
	}

	res = internal.Compare(r2.End, intersection.End)
	if res < 0 {
		intersection.End = r2.End
		intersection.EndIncluded = r2.EndIncluded
	} else if res == 0 {
		intersection.EndIncluded = intersection.EndIncluded && r2.EndIncluded
	} else if intersection.End == nil {
		intersection.End = r2.End
		intersection.EndIncluded = r2.EndIncluded
	}
	return intersection
}
