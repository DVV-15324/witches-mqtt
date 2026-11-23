package witchesmqtt

import (
	"context"
	"errors"
	"strings"
	"sync"
)

type TopicHandler func(ctx context.Context, topic string, payload []byte) error

var (
	ErrInvalidTopic  = errors.New("invalid MQTT topic")
	ErrInvalidFilter = errors.New("invalid MQTT topic filter")
)

type Router struct {
	mu     sync.RWMutex
	routes []route
}

type route struct {
	pattern string
	levels  []string
	handler TopicHandler
}

func NewRouter() *Router {
	return &Router{}
}

// Handle preserves the original API.
// Invalid registrations are ignored. Use HandleChecked when validation
// errors need to be handled explicitly.
func (r *Router) Handle(pattern string, h TopicHandler) *Router {
	_ = r.HandleChecked(pattern, h)
	return r
}

// HandleChecked validates and registers an MQTT topic filter.
func (r *Router) HandleChecked(pattern string, h TopicHandler) error {
	if r == nil {
		return errors.New("router is nil")
	}
	if h == nil {
		return errors.New("topic handler is nil")
	}

	levels, err := validateFilter(pattern)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Prevent accidental duplicate registration.
	for _, existing := range r.routes {
		if existing.pattern == pattern {
			return errors.New("duplicate MQTT topic filter: " + pattern)
		}
	}

	r.routes = append(r.routes, route{
		pattern: pattern,
		levels:  levels,
		handler: h,
	})

	return nil
}

func (r *Router) Patterns() []string {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, len(r.routes))
	for i, rt := range r.routes {
		out[i] = rt.pattern
	}
	return out
}

func (r *Router) Dispatch(
	ctx context.Context,
	topic string,
	payload []byte,
) error {
	if r == nil {
		return errors.New("router is nil")
	}

	levels, err := validateTopic(topic)
	if err != nil {
		return err
	}

	r.mu.RLock()

	var best TopicHandler
	bestScore := -1

	for _, rt := range r.routes {
		if score, ok := matchFilter(rt.levels, levels); ok {
			if score > bestScore {
				best = rt.handler
				bestScore = score
			}
		}
	}

	r.mu.RUnlock()

	// No route matched: keep original behavior.
	if best == nil {
		return nil
	}

	return best(ctx, topic, payload)
}

// validateFilter validates MQTT wildcard syntax.
// '+' must occupy an entire level.
// '#' must occupy an entire level and be the final level.
func validateFilter(filter string) ([]string, error) {
	if filter == "" || strings.ContainsRune(filter, '\x00') {
		return nil, ErrInvalidFilter
	}

	levels := strings.Split(filter, "/")

	for i, level := range levels {
		if strings.Contains(level, "#") && level != "#" {
			return nil, ErrInvalidFilter
		}
		if strings.Contains(level, "+") && level != "+" {
			return nil, ErrInvalidFilter
		}
		if level == "#" && i != len(levels)-1 {
			return nil, ErrInvalidFilter
		}
	}

	return levels, nil
}

// Topic names cannot contain MQTT wildcards.
func validateTopic(topic string) ([]string, error) {
	if topic == "" || strings.ContainsRune(topic, '\x00') {
		return nil, ErrInvalidTopic
	}

	if strings.ContainsAny(topic, "+#") {
		return nil, ErrInvalidTopic
	}

	return strings.Split(topic, "/"), nil
}

// matchFilter returns whether the filter matches the topic and
// a specificity score used to select a single handler.
//
// Literal level: +10
// '+' wildcard:  +1
// '#' wildcard:   0
// Exact match:   +2 bonus
//
// Equal scores preserve registration order.
func matchFilter(filter, topic []string) (int, bool) {
	score := 0

	for i, f := range filter {
		// Multi-level wildcard: matches zero or more levels.
		if f == "#" {
			// Root wildcard must not match system topics ($...).
			if i == 0 && len(topic) > 0 &&
				strings.HasPrefix(topic[0], "$") {
				return 0, false
			}

			return score, true
		}

		// Topic has fewer levels than the filter.
		if i >= len(topic) {
			return 0, false
		}

		// Single-level wildcard.
		if f == "+" {
			if i == 0 && strings.HasPrefix(topic[i], "$") {
				return 0, false
			}

			score++
			continue
		}

		// Literal level must match exactly.
		if f != topic[i] {
			return 0, false
		}

		score += 10
	}

	// Without '#', the number of levels must be equal.
	if len(filter) != len(topic) {
		return 0, false
	}

	// Exact-length match bonus.
	score += 2

	return score, true
}
