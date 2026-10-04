package metrics

import "fmt"

type Metrics struct{}

func New() *Metrics {
	fmt.Println("[Metrics] Telemetry & observability initialized")
	return &Metrics{}
}

func (m *Metrics) LogTaskDone() {
	fmt.Println("[Metrics] Task completed successfully")
}
