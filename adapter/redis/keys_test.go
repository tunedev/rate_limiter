package rediskey

import (
	"strings"
	"testing"
)

func TestKeyMatchesTheDocumentedLayout(t *testing.T) {
	got := Key("per-ip", "203.0.113.7")
	if want := "rl:v1:{per-ip/203.0.113.7}"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
}

func TestKeyPutsRuleAndSubjectInOneHashTag(t *testing.T) {
	got := Key("per-ip", "203.0.113.7")

	open, close := strings.Index(got, "{"), strings.Index(got, "}")
	if open < 0 || close < 0 || close < open {
		t.Fatalf("Key = %q, want a single hash tag", got)
	}
	if tag := got[open+1 : close]; tag != "per-ip/203.0.113.7" {
		t.Fatalf("hash tag = %q, want the rule and subject together", tag)
	}
}

func TestKeySeparatesRulesOverOneSubject(t *testing.T) {
	if Key("route", "user:42") == Key("tenant", "user:42") {
		t.Fatal("two rules over one subject share a key, want them separate")
	}
}

func TestKeySeparatesComponentsContainingTheSeparator(t *testing.T) {
	if Key("a/b", "c") == Key("a", "b/c") {
		t.Fatal("a slash in a component makes two distinct pairs share a key, want them separate")
	}
	if Key("a", "b") == Key("a%2Fb", "") {
		t.Fatal("an escape sequence in a component collides with the escaping of a slash")
	}
}
