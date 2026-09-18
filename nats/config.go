package nats

import (
	"fmt"
	"log/slog"
	"runtime"
	"strconv"
	"time"

	natsio "github.com/nats-io/nats.go"

	"github.com/Paymentbox-com/service-mesh-nats-go/mesh"
)

// Configuration keys this runtime reads from a mesh.Config, beyond
// mesh.DeploymentGroupKey. Every value is a string.
const (
	// URLKey is the NATS server URL or a comma-separated list. Default
	// natsio.DefaultURL.
	URLKey = "url"

	// NameKey is the connection name reported to the server. Optional.
	NameKey = "name"

	// ConnectTimeoutKey bounds the initial connection, as a Go duration
	// string such as "5s". Default "5s".
	ConnectTimeoutKey = "connect_timeout"

	// RequestTimeoutKey is the runtime's own bound on a Request when the
	// caller's context carries no deadline, as a Go duration string. Default
	// "30s". Also accepted in the per-call options of Request, where it
	// overrides the configured value for that call.
	RequestTimeoutKey = "request_timeout"

	// ConcurrencyKey is the maximum number of handlers running at once across
	// all endpoints and subscribers, as a positive integer. Default is the
	// CPU count.
	ConcurrencyKey = "concurrency"
)

const (
	defaultConnectTimeout = 5 * time.Second
	defaultRequestTimeout = 30 * time.Second
)

// Option adjusts settings that cannot be expressed as strings in a
// mesh.Config.
type Option func(*settings)

// WithLogger sets the logger that receives handler failures and reply or
// flush errors. Default slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(s *settings) { s.logger = l }
}

// WithNATSOptions appends options to the natsio.Connect call, after the ones
// derived from the mesh.Config, so they can override them.
func WithNATSOptions(opts ...natsio.Option) Option {
	return func(s *settings) { s.natsOpts = append(s.natsOpts, opts...) }
}

// settings is a parsed mesh.Config plus Options.
type settings struct {
	url             string
	name            string
	deploymentGroup string
	connectTimeout  time.Duration
	requestTimeout  time.Duration
	concurrency     int
	logger          *slog.Logger
	natsOpts        []natsio.Option
}

// parseSettings validates and converts cfg. requireDeployment is true for a
// Runtime, which returns mesh.ErrNoDeploymentGroup without one, and false
// for a standalone Client, which ignores the key.
func parseSettings(cfg mesh.Config, opts []Option, requireDeployment bool) (settings, error) {
	s := settings{
		url:            natsio.DefaultURL,
		connectTimeout: defaultConnectTimeout,
		requestTimeout: defaultRequestTimeout,
		concurrency:    runtime.NumCPU(),
		logger:         slog.Default(),
	}

	if v, ok := cfg[URLKey]; ok && v != "" {
		s.url = v
	}
	s.name = cfg[NameKey]
	s.deploymentGroup = cfg[mesh.DeploymentGroupKey]
	if requireDeployment && s.deploymentGroup == "" {
		return settings{}, mesh.ErrNoDeploymentGroup
	}

	var err error
	if s.connectTimeout, err = durationSetting(cfg, ConnectTimeoutKey, s.connectTimeout); err != nil {
		return settings{}, err
	}
	if s.requestTimeout, err = durationSetting(cfg, RequestTimeoutKey, s.requestTimeout); err != nil {
		return settings{}, err
	}
	if v, ok := cfg[ConcurrencyKey]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return settings{}, fmt.Errorf("%w: %s=%q must be a positive integer", ErrBadConfig, ConcurrencyKey, v)
		}
		s.concurrency = n
	}

	for _, o := range opts {
		o(&s)
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	return s, nil
}

// durationSetting reads key from cfg as a positive Go duration, returning
// fallback when the key is absent.
func durationSetting(cfg map[string]string, key string, fallback time.Duration) (time.Duration, error) {
	v, ok := cfg[key]
	if !ok {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%w: %s=%q must be a positive duration such as \"5s\"", ErrBadConfig, key, v)
	}
	return d, nil
}

func (s settings) connect() (*natsio.Conn, error) {
	opts := []natsio.Option{natsio.Timeout(s.connectTimeout)}
	if s.name != "" {
		opts = append(opts, natsio.Name(s.name))
	}
	opts = append(opts, s.natsOpts...)
	return natsio.Connect(s.url, opts...)
}
