package executor

import (
	"sync"

	"github.com/dshills/QuantaDB/internal/sql/parser"
)

// ExpressionCache caches parsed expressions to avoid re-parsing on every evaluation
type ExpressionCache struct {
	cache map[string]parser.Expression
	mu    sync.RWMutex
}

// globalExprCache is a shared expression cache for CHECK constraints
var globalExprCache = &ExpressionCache{
	cache: make(map[string]parser.Expression),
}

// GetOrParse retrieves a cached expression or parses it if not cached
func (c *ExpressionCache) GetOrParse(expr string) (parser.Expression, error) {
	// Try to get from cache first
	c.mu.RLock()
	if cached, ok := c.cache[expr]; ok {
		c.mu.RUnlock()
		return cached, nil
	}
	c.mu.RUnlock()

	// Parse expression - add parentheses if not already present
	// This is needed because ParseExpression expects a complete expression
	exprToParse := expr
	if len(expr) > 0 && expr[0] != '(' {
		exprToParse = "(" + expr + ")"
	}

	p := parser.NewParser(exprToParse)
	parsed, err := p.ParseExpression()
	if err != nil {
		return nil, err
	}

	// Store in cache
	c.mu.Lock()
	c.cache[expr] = parsed
	c.mu.Unlock()

	return parsed, nil
}

// Clear removes all cached expressions
func (c *ExpressionCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]parser.Expression)
}
