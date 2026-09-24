package credentials

import (
	"bufio"
	"crypto/subtle"
	"fmt"
	"io"
	"os"
	"strings"
)

const DefaultPath = "config/binance_credentials.enc"

var allowedKeys = map[string]bool{
	"BINANCE_TESTNET_API_KEY":    true,
	"BINANCE_TESTNET_API_SECRET": true,
	"BINANCE_MAINNET_API_KEY":    true,
	"BINANCE_MAINNET_API_SECRET": true,
}

type EnvironmentCredentials struct {
	APIKey    string `json:"-"`
	APISecret string `json:"-"`
}

func (c EnvironmentCredentials) Available() bool {
	return strings.TrimSpace(c.APIKey) != "" && strings.TrimSpace(c.APISecret) != ""
}

func (EnvironmentCredentials) String() string { return "{APIKey:REDACTED APISecret:REDACTED}" }

type CredentialStore struct {
	Testnet EnvironmentCredentials `json:"-"`
	Mainnet EnvironmentCredentials `json:"-"`
}

type Presence struct {
	TestnetAPIKeyPresent bool `json:"testnet_api_key_present"`
	TestnetSecretPresent bool `json:"testnet_secret_present"`
	MainnetAPIKeyPresent bool `json:"mainnet_api_key_present"`
	MainnetSecretPresent bool `json:"mainnet_secret_present"`
}

func (s CredentialStore) Presence() Presence {
	return Presence{
		TestnetAPIKeyPresent: strings.TrimSpace(s.Testnet.APIKey) != "",
		TestnetSecretPresent: strings.TrimSpace(s.Testnet.APISecret) != "",
		MainnetAPIKeyPresent: strings.TrimSpace(s.Mainnet.APIKey) != "",
		MainnetSecretPresent: strings.TrimSpace(s.Mainnet.APISecret) != "",
	}
}

type Provider interface {
	Load() (CredentialStore, error)
	Path() string
}

type FileProvider struct{ FilePath string }

func NewFileProvider() FileProvider {
	path := strings.TrimSpace(os.Getenv("BINANCE_CREDENTIAL_FILE"))
	if path == "" {
		path = DefaultPath
	}
	return FileProvider{FilePath: path}
}

func (p FileProvider) Path() string { return p.FilePath }

func (p FileProvider) Load() (CredentialStore, error) {
	f, err := os.Open(p.FilePath)
	if err != nil {
		return CredentialStore{}, err
	}
	defer f.Close()
	return Parse(f)
}

func Parse(r io.Reader) (CredentialStore, error) {
	values := map[string]string{}
	scanner := bufio.NewScanner(r)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		raw := scanner.Text()
		if lineNo == 1 {
			raw = strings.TrimPrefix(raw, "\uFEFF")
		}
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(raw, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" || !allowedKeys[key] {
			return CredentialStore{}, fmt.Errorf("invalid credential configuration at line %d", lineNo)
		}
		if value != strings.TrimSpace(value) {
			return CredentialStore{}, fmt.Errorf("credential value has surrounding whitespace at line %d", lineNo)
		}
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			return CredentialStore{}, fmt.Errorf("quoted credential value is unsupported at line %d", lineNo)
		}
		if _, exists := values[key]; exists {
			return CredentialStore{}, fmt.Errorf("duplicate credential key at line %d", lineNo)
		}
		values[key] = value
	}
	if err := scanner.Err(); err != nil {
		return CredentialStore{}, err
	}
	return CredentialStore{
		Testnet: EnvironmentCredentials{APIKey: values["BINANCE_TESTNET_API_KEY"], APISecret: values["BINANCE_TESTNET_API_SECRET"]},
		Mainnet: EnvironmentCredentials{APIKey: values["BINANCE_MAINNET_API_KEY"], APISecret: values["BINANCE_MAINNET_API_SECRET"]},
	}, nil
}

func Redact(message string, secrets ...string) string {
	redacted := message
	for _, secret := range secrets {
		secret = strings.TrimSpace(secret)
		if secret != "" {
			redacted = strings.ReplaceAll(redacted, secret, "REDACTED")
		}
	}
	return redacted
}

func EqualSecret(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
