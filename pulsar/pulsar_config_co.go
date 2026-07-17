package pulsar

// AuthModeMachineIdentity enables secret-less AuthN token-exchange (Machine
// Identity). When auth_mode is unset or any other value, the legacy OAuth2
// client-credentials flow is used (requires auth_client_secret).
const AuthModeMachineIdentity = "machine_identity"

type PulsarConf struct {
	SubscriptionName string `json:"name"`
	Url              string `json:"url"`
	Topic            string `json:"topic"`
	ForceRestart     bool   `json:"force_restart"`
	ReadNewest       bool   `json:"read_newest"`
	SeekByTime       int64  `json:"seek_by_time"`
	AuthClientId     string `json:"client_id"`
	AuthClientSecret string `json:"auth_client_secret"`
	AuthIssuerURL    string `json:"auth_issuer_url"`
	AuthAudience     string `json:"auth_audience"`
	SubscriptionType string `json:"subscription_type"`

	// AuthMode selects how the client authenticates to Pulsar/Viesti.
	// Empty or "client_credentials" => legacy secret-based OAuth2 flow.
	// "machine_identity" => secret-less token-exchange flow.
	AuthMode string `json:"auth_mode"`

	// The fields below configure the machine_identity (token-exchange) flow.
	// For machine_identity, AuthIssuerURL is the full HTTPS token-exchange
	// endpoint. The request body is described by AuthExchangeForm; the subject
	// token is injected at runtime under AuthSubjectTokenField.

	// AuthSubjectTokenPath is the path to the native (K8s) identity token that
	// is exchanged for a downstream token. Deployment-specific; required in
	// machine_identity mode (no default).
	AuthSubjectTokenPath string `json:"auth_subject_token_path"`
	// AuthSubjectTokenField is the form field name under which the subject token
	// is sent. Defaults to "subject_token" when unset.
	AuthSubjectTokenField string `json:"auth_subject_token_field"`
	// AuthExchangeForm is the set of static form fields POSTed to the token
	// endpoint (e.g. grant_type, client_id, target_client_id or audience). The
	// subject token is injected at runtime under AuthSubjectTokenField.
	AuthExchangeForm map[string]string `json:"auth_exchange_form"`
}
