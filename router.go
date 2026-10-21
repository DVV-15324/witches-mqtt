package witchesmqtt

import (
	"context"
	"strings"
	"sync"
)

type TopicHandler func(ctx context.Context, topic string, payload []byte) error

type Router struct {
	mu     sync.RWMutex
	routes []route
}

type route struct {
	pattern string
	levels  []string
	handler TopicHandler
}

func NewRouter() *Router { return &Router{} }

func (r *Router) Handle(pattern string, h TopicHandler) *Router { // <- đổi
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes = append(r.routes, route{
		pattern: pattern,
		levels:  strings.Split(pattern, "/"),
		handler: h,
	})
	return r
}

func (r *Router) Patterns() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.routes))
	for i, rt := range r.routes {
		out[i] = rt.pattern
	}
	return out
}

func (r *Router) Dispatch(ctx context.Context, topic string, payload []byte) error {
	levels := strings.Split(topic, "/")
	r.mu.RLock()
	var best TopicHandler // <- đổi
	bestScore := -1
	for _, rt := range r.routes {
		if score, ok := scoreLevels(rt.levels, levels); ok && score > bestScore {
			best = rt.handler
			bestScore = score
		}
	}
	r.mu.RUnlock()
	if best == nil {
		return nil
	}
	return best(ctx, topic, payload)
}

func scoreLevels(pattern, topic []string) (int, bool) {
	score := 0
	for i, p := range pattern {
		if p == "#" {
			return score, true
		}
		if i >= len(topic) {
			return 0, false
		}
		if p == "+" {
			score++
			continue
		}
		if p != topic[i] {
			return 0, false
		}
		score += 10
	}
	if len(pattern) != len(topic) {
		return 0, false
	}
	return score, true
}
