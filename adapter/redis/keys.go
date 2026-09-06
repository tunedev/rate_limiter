// Package rediskey builds the keys both Redis store adapters use.
package rediskey

import (
	"net/url"

	"github.com/tunedev/rate_limiter/domain"
)

// Key returns the key holding one rule's state for one subject. Both
// components are percent-escaped before joining, so the separator is
// unambiguous and no two distinct pairs share a key. The hash tag wraps both,
// so a rule and subject land in one cluster slot; nothing needs to colocate
// beyond that, because the store contract forbids cross-key atomicity.
func Key(rule domain.RuleID, key domain.Key) string {
	return "rl:v1:{" + url.PathEscape(string(rule)) + "/" + url.PathEscape(string(key)) + "}"
}
