package types

import (
	"time"
)

type Entry struct {
	Value     string
	ExpiresAt time.Time
}

type StoreMap = map[string]*Entry

func (e *Entry) IsExpired(now time.Time) bool {
	if e.ExpiresAt.IsZero() {
		return false
	}

	return e.ExpiresAt.Before(now)
}
