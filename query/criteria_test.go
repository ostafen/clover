package query_test

import (
	"testing"

	"github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/query"
)

func TestCriteriaNormalizeValues(t *testing.T) {
	doc := document.NewDocumentOf(map[string]interface{}{
		"age": 20, "ages": []int{20}, "other": 20,
	})
	for name, value := range map[string]interface{}{
		"int32": int32(20), "uint32": uint32(20), "float32": float32(20),
		"string reference": "$other", "field reference": query.Field("other"),
	} {
		for op, criteria := range map[string]query.Criteria{
			"Eq":       query.Field("age").Eq(value),
			"In":       query.Field("age").In(value),
			"Contains": query.Field("ages").Contains(value),
		} {
			t.Run(name+"/"+op, func(t *testing.T) {
				if !criteria.Satisfy(doc) {
					t.Fatal("expected matching normalized value")
				}
			})
		}
	}
	for name, criteria := range map[string]query.Criteria{
		"typed slice Eq": query.Field("ages").Eq([]int32{20}),
		"typed slice In": query.Field("ages").In([]int32{20}),
	} {
		t.Run(name, func(t *testing.T) {
			if !criteria.Satisfy(doc) {
				t.Fatal("expected matching normalized slice")
			}
		})
	}
	for name, criteria := range map[string]query.Criteria{
		"Eq mismatch":          query.Field("age").Eq(int32(21)),
		"In mismatch":          query.Field("age").In(int32(21)),
		"Contains mismatch":    query.Field("ages").Contains(int32(21)),
		"Eq unsupported":       query.Field("age").Eq(make(chan int)),
		"In unsupported":       query.Field("age").In(make(chan int)),
		"Contains unsupported": query.Field("ages").Contains(make(chan int)),
	} {
		t.Run(name, func(t *testing.T) {
			if criteria.Satisfy(doc) {
				t.Fatal("unexpected match")
			}
		})
	}
}
