package nats

import (
	"fmt"
	"log/slog"
	"runtime"
	"strconv"
	"time"

	natsio "github.com/nats-io/nats.go"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// Configuration keys this package reads from a mesh.Config. Every value is a
// string. NewClient reads URLKey, NameKey, ConnectTimeoutKey, and
// RequestTimeoutKey. New reads mesh.DeploymentGroupKey and ConcurrencyKey.
// Each constructor ignores the other's keys, so one Config can be given to
// both.
const (
	// URLKey is the NATS server URL or a comma-separated list. Default
	// natsio.DefaultURL.
	URLKey = "url"

	// NameKey is the connection name reported to the server. Optional.
	NameKey = "name"

	// ConnectTimeoutKey bounds the initial connection, as a Go duration
	// string such as "5s". Default "5s".
	ConnectTimeoutKey = "connect_timeout"

	// RequestTimeoutKey is the client's own bound on a Request when the
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

// Option carries a setting that cannot be expressed as a string in a
// mesh.Config. New reads WithLogger and NewClient reads WithNATSOptions; each
// constructor ignores the other's option.
type Option func(*options)

// WithLogger sets the logger that receives handler failures and reply or
// flush errors. Default slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithNATSOptions appends options to the natsio.Connect call, after the ones
// derived from the mesh.Config, so they can override them.
func WithNATSOptions(opts ...natsio.Option) Option {
	return func(o *options) { o.natsOpts = append(o.natsOpts, opts...) }
}

type options struct {
	logger   *slog.Logger
	natsOpts []natsio.Option
}

func applyOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// clientSettings is what NewClient reads from a mesh.Config and Options.
type clientSettings struct {
	url            string
	name           string
	connectTimeout time.Duration
	requestTimeout time.Duration
	natsOpts       []natsio.Option
}

func parseClientSettings(cfg mesh.Config, opts []Option) (clientSettings, error) {
	s := clientSettings{
		url:            natsio.DefaultURL,
		connectTimeout: defaultConnectTimeout,
		requestTimeout: defaultRequestTimeout,
		natsOpts:       applyOptions(opts).natsOpts,
	}
	if v, ok := cfg[URLKey]; ok && v != "" {
		s.url = v
	}
	s.name = cfg[NameKey]

	var err error
	if s.connectTimeout, err = durationSetting(cfg, ConnectTimeoutKey, s.connectTimeout); err != nil {
		return clientSettings{}, err
	}
	if s.requestTimeout, err = durationSetting(cfg, RequestTimeoutKey, s.requestTimeout); err != nil {
		return clientSettings{}, err
	}
	return s, nil
}

func (s clientSettings) connect() (*natsio.Conn, error) {
	opts := []natsio.Option{natsio.Timeout(s.connectTimeout)}
	if s.name != "" {
		opts = append(opts, natsio.Name(s.name))
	}
	opts = append(opts, s.natsOpts...)
	return natsio.Connect(s.url, opts...)
}

// runtimeSettings is what New reads from a mesh.Config and Options.
type runtimeSettings struct {
	deploymentGroup string
	concurrency     int
	logger          *slog.Logger
}

// parseRuntimeSettings returns mesh.ErrNoDeploymentGroup when
// mesh.DeploymentGroupKey is absent or empty.
func parseRuntimeSettings(cfg mesh.Config, opts []Option) (runtimeSettings, error) {
	s := runtimeSettings{
		deploymentGroup: cfg[mesh.DeploymentGroupKey],
		concurrency:     runtime.NumCPU(),
		logger:          applyOptions(opts).logger,
	}
	if s.deploymentGroup == "" {
		return runtimeSettings{}, mesh.ErrNoDeploymentGroup
	}
	if v, ok := cfg[ConcurrencyKey]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return runtimeSettings{}, fmt.Errorf("%w: %s=%q must be a positive integer", ErrBadConfig, ConcurrencyKey, v)
		}
		s.concurrency = n
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
