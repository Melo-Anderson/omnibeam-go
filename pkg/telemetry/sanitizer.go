package telemetry

import (
	"fmt"
	"regexp"
	"strings"
)

var defaultSensitiveKeywords = []string{
	"password", "secret", "token", "apikey", "api_key", "auth", "credential", "private_key",
}

var jsonSecretPattern = regexp.MustCompile(`(?i)"([^"]*(?:password|secret|token|apikey|api_key|auth|credential|cpf)[^"]*)"\s*:\s*"([^"]+)"`)

// IsSensitiveKey reports whether a field key represents a sensitive credential or PII field.
func IsSensitiveKey(key string, customSensitive []string) bool {
	norm := strings.ToLower(strings.TrimSpace(key))
	for _, kw := range defaultSensitiveKeywords {
		if strings.Contains(norm, kw) {
			return true
		}
	}
	for _, custom := range customSensitive {
		if norm == strings.ToLower(strings.TrimSpace(custom)) {
			return true
		}
	}
	return false
}

// SanitizeValue returns "[REDACTED]" if key is sensitive, otherwise returns val unchanged.
func SanitizeValue(key string, val any, customSensitive []string) any {
	if IsSensitiveKey(key, customSensitive) {
		return "[REDACTED]"
	}
	return val
}

// SanitizePayload replaces sensitive key values inside raw JSON payloads with "[REDACTED]".
func SanitizePayload(payload string, customSensitive []string) string {
	res := jsonSecretPattern.ReplaceAllString(payload, `"$1":"[REDACTED]"`)
	for _, custom := range customSensitive {
		if custom == "" {
			continue
		}
		pattern := regexp.MustCompile(fmt.Sprintf(`(?i)"(%s)"\s*:\s*"([^"]+)"`, regexp.QuoteMeta(custom)))
		res = pattern.ReplaceAllString(res, `"$1":"[REDACTED]"`)
	}
	return res
}

// ContainsRedacted reports whether s contains the sentinel "[REDACTED]".
func ContainsRedacted(s string) bool {
	return strings.Contains(s, "[REDACTED]")
}
