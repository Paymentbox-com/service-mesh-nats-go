package nats

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// subject assembles a target's segments into a NATS subject or returns
// mesh.ErrInvalidTarget wrapped with the reason.
func subject(t mesh.Target) (string, error) {
	if len(t.Segments) == 0 {
		return "", fmt.Errorf("%w: no segments", mesh.ErrInvalidTarget)
	}
	for i, seg := range t.Segments {
		if err := checkSegment(seg); err != nil {
			return "", fmt.Errorf("%w: segment %d %q: %v", mesh.ErrInvalidTarget, i, seg, err)
		}
	}
	return strings.Join(t.Segments, "."), nil
}

func checkSegment(seg string) error {
	if seg == "" {
		return fmt.Errorf("empty")
	}
	for _, c := range seg {
		switch {
		case c == '.':
			return fmt.Errorf("contains '.'")
		case c == '*' || c == '>':
			return fmt.Errorf("contains wildcard %q", c)
		case unicode.IsSpace(c):
			return fmt.Errorf("contains whitespace")
		case !unicode.IsPrint(c):
			return fmt.Errorf("contains non-printable character")
		}
	}
	return nil
}

// checkKind returns mesh.ErrKindMismatch wrapped with both kinds when t's
// kind is not want.
func checkKind(t mesh.Target, want mesh.Kind, use string) error {
	if t.Kind != want {
		return fmt.Errorf("%w: %s requires a %s target, got %s", mesh.ErrKindMismatch, use, want, t.Kind)
	}
	return nil
}

// consumerGroup resolves the NATS queue group for a binding. The binding's
// own metadata wins, then the target's metadata, then the deployment group.
// The empty string means a plain subscription with no group.
func consumerGroup(binding, target map[string]string, deploymentGroup string) string {
	for _, md := range [2]map[string]string{binding, target} {
		v, set := md[mesh.ConsumerGroupKey]
		switch {
		case !set || v == "":
			continue
		case v == mesh.ConsumerGroupNone:
			return ""
		default:
			return v
		}
	}
	return deploymentGroup
}
