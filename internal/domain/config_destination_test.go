package domain_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestEncryptionConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     domain.EncryptionConfig
		wantErr bool
	}{
		{"none is valid", domain.EncryptionConfig{Type: "none"}, false},
		{"empty defaults to none", domain.EncryptionConfig{}, false},
		{"pgp without key ref fails", domain.EncryptionConfig{Type: "pgp"}, true},
		{"pgp with key ref passes", domain.EncryptionConfig{Type: "pgp", PublicKeyRef: "vault:pgp/key"}, false},
		{"kms without key ref fails", domain.EncryptionConfig{Type: "kms"}, true},
		{"kms with key ref passes", domain.EncryptionConfig{Type: "kms", KMSKeyRef: "projects/p/keys/k"}, false},
		{"unknown type fails", domain.EncryptionConfig{Type: "rsa"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.ApplyDefaults()
			err := tc.cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
