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
// RequestTimeoutKey. New reads mesh.DeploymentGroupKey, ConcurrencyKey,
// MaxDeliverKey, and the pull keys such as PullMaxMessagesKey.
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

	// RequestTimeoutKey bounds a Request, and the wait for a JetStream
	// publish ack, as a Go duration string. Default "30s". A context deadline
	// that comes sooner bounds the call instead. Also read from the
	// settings of Request and Publish, which are their per-call options over
	// their Target's Metadata, where it overrides the configured value for
	// that call.
	RequestTimeoutKey = "request_timeout"

	// ConcurrencyKey is the maximum number of handlers running at once across
	// all endpoints and subscribers, as a positive integer. Default is the
	// CPU count.
	ConcurrencyKey = "concurrency"

	// MaxDeliverKey is the most times one JetStream message is delivered, as
	// a positive integer. In a Config it is the default for subscribers that
	// do not set it, and its own default is 5. It is also a subscriber
	// option.
	MaxDeliverKey = "max_deliver"
)

// JetStream options, read from settings. A Request's or Publish's settings
// are its per-call options over its Target's Metadata, and an Endpoint's or
// Subscriber's are its Metadata over its Target's Metadata. StreamKey is read
// by Request, Publish, and New. The others are read from the settings of a
// Subscriber that sets StreamKey, along with MaxDeliverKey and the pull keys.
const (
	// StreamKey names a JetStream stream. On Publish it sends a JetStream
	// publish that expects this stream and waits for its ack. On a
	// Subscriber it consumes from a consumer on this stream. On an Endpoint
	// or a Request it is ErrBadConfig.
	StreamKey = "stream"

	// CreateConsumerKey is "true" or "false". Default "true". "false" leaves
	// a named consumer group's consumer to be created outside the runtime.
	CreateConsumerKey = "create_consumer"

	// AckPolicyKey is "double", "explicit", or "none". Default "double".
	// "double" acks each message and waits for the server to confirm it,
	// "explicit" acks without waiting, and "none" acks nothing.
	AckPolicyKey = "ack_policy"

	// AckWaitKey is how long the server waits for an ack before it sends a
	// message again, as a Go duration string. Default "30s".
	AckWaitKey = "ack_wait"

	// BackoffKey is a comma-separated list of Go duration strings that
	// replaces AckWaitKey for each redelivery in turn. MaxDeliverKey must
	// exceed its length.
	BackoffKey = "backoff"

	// NakDelayKey is how long the server waits before it sends a failed
	// message again, as a Go duration string. Unset sends it again right
	// away.
	NakDelayKey = "nak_delay"

	// MaxAckPendingKey is the most unacked messages the consumer allows at
	// once, as a positive integer. Default 1000.
	MaxAckPendingKey = "max_ack_pending"

	// DeliverPolicyKey is "all" or "new", where a newly created consumer
	// starts. Default "all".
	DeliverPolicyKey = "deliver_policy"

	// ConsumerInactiveThresholdKey is how long a consumer outlives its last
	// puller, as a Go duration string. Default "168h" for a named consumer
	// group, "5m" for a consumer group of mesh.ConsumerGroupNone with
	// AckPolicyKey "none", and the server's default otherwise.
	ConsumerInactiveThresholdKey = "consumer_inactive_threshold"
)

// Pull keys set how a JetStream subscriber pulls messages from its consumer.
// They stay in the client and are never sent as consumer settings. Each is
// read from a subscriber's settings, and in a Config it is the default for
// subscribers that do not set it. Unset keys take nats.go's defaults. nats.go
// checks the limits given below when Start begins pulling, and a value
// outside them makes Start return its error.
const (
	// PullMaxMessagesKey is the most messages the subscriber asks for in one
	// pull and holds in its buffer, as a positive integer. Default 500.
	PullMaxMessagesKey = "pull_max_messages"

	// PullMaxBytesKey is the most bytes the subscriber asks for in one pull
	// and holds in its buffer, as a positive integer. Without
	// PullMaxMessagesKey it replaces the message limit. With it, it caps the
	// bytes of each batch. It must hold the largest message, or the
	// subscriber stalls. Default none.
	PullMaxBytesKey = "pull_max_bytes"

	// PullThresholdMessagesKey is how few messages still owed from earlier
	// pulls makes the subscriber pull again, as a positive integer. Default
	// half of PullMaxMessagesKey.
	PullThresholdMessagesKey = "pull_threshold_messages"

	// PullExpiryKey is how long the server keeps a pull open while it has
	// nothing to send, as a Go duration string of at least "1s". Default
	// "30s".
	PullExpiryKey = "pull_expiry"

	// PullHeartbeatKey is how often the server signals an open pull that has
	// nothing to send, as a Go duration string from "500ms" to "30s" and no
	// more than half of PullExpiryKey. Two missed heartbeats make the
	// subscriber pull again. Default half of PullExpiryKey, or "5s" for an
	// ordered consumer.
	PullHeartbeatKey = "pull_heartbeat"
)

const (
	defaultConnectTimeout = 5 * time.Second
	defaultRequestTimeout = 30 * time.Second
	defaultMaxDeliver     = 5
)

// Option carries a setting that cannot be expressed as a string in a
// mesh.Config. New reads WithLogger.
type Option func(*options)

// WithLogger sets the logger that receives handler failures and reply or
// flush errors. Default slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

type options struct {
	logger *slog.Logger
}

func applyOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// clientSettings is what NewClient reads from a mesh.Config.
type clientSettings struct {
	url            string
	name           string
	connectTimeout time.Duration
	requestTimeout time.Duration
}

func parseClientSettings(cfg mesh.Config) (clientSettings, error) {
	s := clientSettings{
		url:            natsio.DefaultURL,
		connectTimeout: defaultConnectTimeout,
		requestTimeout: defaultRequestTimeout,
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
	return natsio.Connect(s.url, opts...)
}

// runtimeSettings is what New reads from a mesh.Config and Options.
type runtimeSettings struct {
	deploymentGroup string
	concurrency     int
	maxDeliver      int
	pull            pullSettings
	logger          *slog.Logger
}

// parseRuntimeSettings returns mesh.ErrNoDeploymentGroup when
// mesh.DeploymentGroupKey is absent or empty.
func parseRuntimeSettings(cfg mesh.Config, opts []Option) (runtimeSettings, error) {
	s := runtimeSettings{
		deploymentGroup: cfg[mesh.DeploymentGroupKey],
		concurrency:     runtime.NumCPU(),
		maxDeliver:      defaultMaxDeliver,
		logger:          applyOptions(opts).logger,
	}
	if s.deploymentGroup == "" {
		return runtimeSettings{}, mesh.ErrNoDeploymentGroup
	}
	var err error
	if s.concurrency, err = positiveIntSetting(cfg, ConcurrencyKey, s.concurrency); err != nil {
		return runtimeSettings{}, err
	}
	if s.maxDeliver, err = positiveIntSetting(cfg, MaxDeliverKey, s.maxDeliver); err != nil {
		return runtimeSettings{}, err
	}
	if s.pull, err = readPullSettings(cfg, pullSettings{}); err != nil {
		return runtimeSettings{}, err
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

// positiveIntSetting reads key from cfg as a positive integer, returning
// fallback when the key is absent.
func positiveIntSetting(cfg map[string]string, key string, fallback int) (int, error) {
	v, ok := cfg[key]
	if !ok {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: %s=%q must be a positive integer", ErrBadConfig, key, v)
	}
	return n, nil
}
