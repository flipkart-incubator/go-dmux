package metrics

type Registry struct {
	provider RegistryProvider
}

// RegistryProvider interface that implements metric registry types
type RegistryProvider interface {
	init()
}

//Start creates a registry and initializes the metrics based on the registry type and implementation and returns the created registry
func Start(metricPort int) *Registry {
	config := &PrometheusConfig{metricPort: metricPort}

	reg := &Registry{provider: config}
	reg.provider.init()
	return reg
}
