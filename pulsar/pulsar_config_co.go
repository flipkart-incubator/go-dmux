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
	// "machine_identity" => secret-less AuthN token-exchange flow.
	AuthMode string `json:"auth_mode"`
	// AuthTargetClientId is the target client id used in the AuthN
	// token-exchange request (machine_identity mode only).
	AuthTargetClientId string `json:"auth_target_client_id"`
	// AuthSubjectTokenPath is the path to the native (K8s) identity token
	// that is exchanged for an AuthN token (machine_identity mode only).
	// Deployment-specific; supplied via tenant config, not hardcoded here.
	AuthSubjectTokenPath string `json:"auth_subject_token_path"`
}
