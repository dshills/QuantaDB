// Package index provides indexing structures for database tables.
package index

import (
	"bytes"
	"fmt"
	"sync"
)

// degree is the minimum degree of the B+Tree (t).
// A node can have at most 2t-1 keys and 2t children.
const degree = 128

// Comparator compares two keys and returns:
// -1 if a < b, 0 if a == b, 1 if a > b.
type Comparator func(a, b []byte) int

// BTree is a B+Tree implementation optimized for database indexing.
type BTree struct {
	mu         sync.RWMutex
	root       *node
	comparator Comparator
	height     int
}

// node represents a node in the B+Tree.
type node struct {
	// For internal nodes: keys are separator values
	// For leaf nodes: keys are actual data keys
	keys [][]byte

	// For internal nodes: pointers to child nodes
	// For leaf nodes: values corresponding to keys
	children []interface{}

	// Link to next leaf (only used in leaf nodes)
	next *node

	// Whether this is a leaf node
	isLeaf bool

	// Parent node (for easier traversal)
	parent *node
}

// NewBTree creates a new B+Tree with the given comparator.
func NewBTree(comparator Comparator) *BTree {
	return &BTree{
		root: &node{
			keys:     make([][]byte, 0, 2*degree-1),
			children: make([]interface{}, 0, 2*degree),
			isLeaf:   true,
		},
		comparator: comparator,
		height:     1,
	}
}

// DefaultComparator provides byte-wise comparison.
func DefaultComparator(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Insert adds a key-value pair to the B+Tree.
func (bt *BTree) Insert(key, value []byte) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Make copies to avoid external modifications
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// If root is full, split it
	if len(bt.root.keys) == 2*degree-1 {
		newRoot := &node{
			keys:     make([][]byte, 0, 2*degree-1),
			children: make([]interface{}, 0, 2*degree),
			isLeaf:   false,
		}
		newRoot.children = append(newRoot.children, bt.root)
		bt.root.parent = newRoot
		bt.splitChild(newRoot, 0)
		bt.root = newRoot
		bt.height++
	}

	bt.insertNonFull(bt.root, keyCopy, valueCopy)
	return nil
}

// insertNonFull inserts a key-value pair into a non-full node.
func (bt *BTree) insertNonFull(n *node, key, value []byte) {
	i := len(n.keys) - 1

	if n.isLeaf {
		// First check for duplicate key
		for j := 0; j < len(n.keys); j++ {
			if bt.comparator(key, n.keys[j]) == 0 {
				// Update existing value
				n.children[j] = value
				return
			}
		}

		// Find position and insert
		n.keys = append(n.keys, nil)
		n.children = append(n.children, nil)

		for i >= 0 && bt.comparator(key, n.keys[i]) < 0 {
			n.keys[i+1] = n.keys[i]
			n.children[i+1] = n.children[i]
			i--
		}

		n.keys[i+1] = key
		n.children[i+1] = value
	} else {
		// Find child to insert into
		for i >= 0 && bt.comparator(key, n.keys[i]) < 0 {
			i--
		}
		i++

		child := n.children[i].(*node)
		if len(child.keys) == 2*degree-1 {
			bt.splitChild(n, i)
			if bt.comparator(key, n.keys[i]) > 0 {
				i++
			}
			child = n.children[i].(*node)
		}

		bt.insertNonFull(child, key, value)
	}
}

// splitChild splits the i-th child of node n.
func (bt *BTree) splitChild(parent *node, i int) {
	fullNode := parent.children[i].(*node)
	newNode := &node{
		keys:     make([][]byte, 0, 2*degree-1),
		children: make([]interface{}, 0, 2*degree),
		isLeaf:   fullNode.isLeaf,
		parent:   parent,
	}

	// Mid point
	mid := degree - 1

	if fullNode.isLeaf {
		// For leaf nodes: split at mid point
		// Left node gets keys/values [0, mid)
		// Right node gets keys/values [mid, 2*degree-1)

		// Copy right half to new node
		newNode.keys = append(newNode.keys, fullNode.keys[mid:]...)
		newNode.children = append(newNode.children, fullNode.children[mid:]...)

		// Trim left node
		fullNode.keys = fullNode.keys[:mid]
		fullNode.children = fullNode.children[:mid]

		// Update linked list pointers
		newNode.next = fullNode.next
		fullNode.next = newNode

		// Promote the first key of the right node to parent
		parent.keys = append(parent.keys, nil)
		copy(parent.keys[i+1:], parent.keys[i:])
		parent.keys[i] = newNode.keys[0]
	} else {
		// For internal nodes: promote middle key
		// Left node gets keys [0, mid) and children [0, mid]
		// Right node gets keys [mid+1, 2*degree-1) and children [mid+1, 2*degree)

		// Promoted key
		promotedKey := fullNode.keys[mid]

		// Copy right half to new node
		newNode.keys = append(newNode.keys, fullNode.keys[mid+1:]...)
		newNode.children = append(newNode.children, fullNode.children[mid+1:]...)

		// Trim left node
		fullNode.keys = fullNode.keys[:mid]
		fullNode.children = fullNode.children[:mid+1]

		// Update parent pointers for moved children
		for _, child := range newNode.children {
			child.(*node).parent = newNode
		}

		// Insert promoted key into parent
		parent.keys = append(parent.keys, nil)
		copy(parent.keys[i+1:], parent.keys[i:])
		parent.keys[i] = promotedKey
	}

	// Insert new node into parent's children
	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = newNode
}

// Search finds a value by key in the B+Tree.
func (bt *BTree) Search(key []byte) ([]byte, bool) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	n := bt.root
	for !n.isLeaf {
		i := 0
		for i < len(n.keys) && bt.comparator(key, n.keys[i]) >= 0 {
			i++
		}
		n = n.children[i].(*node)
	}

	// Search in leaf node
	for i := 0; i < len(n.keys); i++ {
		cmp := bt.comparator(key, n.keys[i])
		if cmp == 0 {
			value := n.children[i].([]byte)
			result := make([]byte, len(value))
			copy(result, value)
			return result, true
		} else if cmp < 0 {
			break
		}
	}

	return nil, false
}

// Delete removes a key from the B+Tree.
func (bt *BTree) Delete(key []byte) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	deleted := bt.deleteFromNode(bt.root, key)

	// If root becomes empty after deletion
	if !bt.root.isLeaf && len(bt.root.keys) == 0 {
		bt.root = bt.root.children[0].(*node)
		bt.root.parent = nil
		bt.height--
	}

	if !deleted {
		return fmt.Errorf("key not found")
	}

	return nil
}

// deleteFromNode removes a key from the subtree rooted at node n.
func (bt *BTree) deleteFromNode(n *node, key []byte) bool {
	if n.isLeaf {
		// Delete from leaf
		for i := 0; i < len(n.keys); i++ {
			if bt.comparator(key, n.keys[i]) == 0 {
				// Remove key and value
				copy(n.keys[i:], n.keys[i+1:])
				copy(n.children[i:], n.children[i+1:])
				n.keys = n.keys[:len(n.keys)-1]
				n.children = n.children[:len(n.children)-1]
				return true
			}
		}
		return false
	}

	// Find child that may contain the key
	i := 0
	for i < len(n.keys) && bt.comparator(key, n.keys[i]) >= 0 {
		i++
	}

	child := n.children[i].(*node)
	deleted := bt.deleteFromNode(child, key)

	if deleted {
		// Handle underflow if necessary
		bt.handleUnderflow(n, i)
	}

	return deleted
}

// handleUnderflow handles the case where a child has too few keys after deletion.
func (bt *BTree) handleUnderflow(parent *node, childIndex int) {
	child := parent.children[childIndex].(*node)
	minKeys := degree - 1
	if child == bt.root {
		minKeys = 1
	}

	if len(child.keys) >= minKeys {
		return // No underflow
	}

	// Try to borrow from left sibling
	if childIndex > 0 {
		leftSibling := parent.children[childIndex-1].(*node)
		if len(leftSibling.keys) > minKeys {
			bt.borrowFromLeft(parent, childIndex)
			return
		}
	}

	// Try to borrow from right sibling
	if childIndex < len(parent.children)-1 {
		rightSibling := parent.children[childIndex+1].(*node)
		if len(rightSibling.keys) > minKeys {
			bt.borrowFromRight(parent, childIndex)
			return
		}
	}

	// Merge with a sibling
	if childIndex > 0 {
		bt.mergeWithLeft(parent, childIndex)
	} else if childIndex < len(parent.children)-1 {
		bt.mergeWithRight(parent, childIndex)
	}
}

// borrowFromLeft borrows a key from the left sibling.
func (bt *BTree) borrowFromLeft(parent *node, childIndex int) {
	child := parent.children[childIndex].(*node)
	leftSibling := parent.children[childIndex-1].(*node)

	if child.isLeaf {
		// For leaf nodes
		child.keys = append([][]byte{leftSibling.keys[len(leftSibling.keys)-1]}, child.keys...)
		child.children = append([]interface{}{leftSibling.children[len(leftSibling.children)-1]}, child.children...)

		leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
		leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]

		parent.keys[childIndex-1] = child.keys[0]
	} else {
		// For internal nodes
		child.keys = append([][]byte{parent.keys[childIndex-1]}, child.keys...)
		parent.keys[childIndex-1] = leftSibling.keys[len(leftSibling.keys)-1]

		child.children = append([]interface{}{leftSibling.children[len(leftSibling.children)-1]}, child.children...)
		leftSibling.children[len(leftSibling.children)-1].(*node).parent = child

		leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
		leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
	}
}

// borrowFromRight borrows a key from the right sibling.
func (bt *BTree) borrowFromRight(parent *node, childIndex int) {
	child := parent.children[childIndex].(*node)
	rightSibling := parent.children[childIndex+1].(*node)

	if child.isLeaf {
		// For leaf nodes
		child.keys = append(child.keys, rightSibling.keys[0])
		child.children = append(child.children, rightSibling.children[0])

		copy(rightSibling.keys, rightSibling.keys[1:])
		copy(rightSibling.children, rightSibling.children[1:])
		rightSibling.keys = rightSibling.keys[:len(rightSibling.keys)-1]
		rightSibling.children = rightSibling.children[:len(rightSibling.children)-1]

		parent.keys[childIndex] = rightSibling.keys[0]
	} else {
		// For internal nodes
		child.keys = append(child.keys, parent.keys[childIndex])
		parent.keys[childIndex] = rightSibling.keys[0]

		child.children = append(child.children, rightSibling.children[0])
		rightSibling.children[0].(*node).parent = child

		copy(rightSibling.keys, rightSibling.keys[1:])
		copy(rightSibling.children, rightSibling.children[1:])
		rightSibling.keys = rightSibling.keys[:len(rightSibling.keys)-1]
		rightSibling.children = rightSibling.children[:len(rightSibling.children)-1]
	}
}

// mergeWithLeft merges a node with its left sibling.
func (bt *BTree) mergeWithLeft(parent *node, childIndex int) {
	child := parent.children[childIndex].(*node)
	leftSibling := parent.children[childIndex-1].(*node)

	if !child.isLeaf {
		// For internal nodes, add separator key from parent
		leftSibling.keys = append(leftSibling.keys, parent.keys[childIndex-1])
	}

	// Merge keys and children
	leftSibling.keys = append(leftSibling.keys, child.keys...)
	leftSibling.children = append(leftSibling.children, child.children...)

	// Update parent pointers for internal nodes
	if !child.isLeaf {
		for _, c := range child.children {
			c.(*node).parent = leftSibling
		}
	}

	// Update next pointer for leaf nodes
	if child.isLeaf {
		leftSibling.next = child.next
	}

	// Remove separator key and child pointer from parent
	copy(parent.keys[childIndex-1:], parent.keys[childIndex:])
	copy(parent.children[childIndex:], parent.children[childIndex+1:])
	parent.keys = parent.keys[:len(parent.keys)-1]
	parent.children = parent.children[:len(parent.children)-1]
}

// mergeWithRight merges a node with its right sibling.
func (bt *BTree) mergeWithRight(parent *node, childIndex int) {
	child := parent.children[childIndex].(*node)
	rightSibling := parent.children[childIndex+1].(*node)

	if !child.isLeaf {
		// For internal nodes, add separator key from parent
		child.keys = append(child.keys, parent.keys[childIndex])
	}

	// Merge keys and children
	child.keys = append(child.keys, rightSibling.keys...)
	child.children = append(child.children, rightSibling.children...)

	// Update parent pointers for internal nodes
	if !child.isLeaf {
		for _, c := range rightSibling.children {
			c.(*node).parent = child
		}
	}

	// Update next pointer for leaf nodes
	if child.isLeaf {
		child.next = rightSibling.next
	}

	// Remove separator key and right child pointer from parent
	copy(parent.keys[childIndex:], parent.keys[childIndex+1:])
	copy(parent.children[childIndex+1:], parent.children[childIndex+2:])
	parent.keys = parent.keys[:len(parent.keys)-1]
	parent.children = parent.children[:len(parent.children)-1]
}

// Range performs a range scan from startKey to endKey (inclusive).
func (bt *BTree) Range(startKey, endKey []byte) ([]KVPair, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	if bt.comparator(startKey, endKey) > 0 {
		return nil, fmt.Errorf("invalid range: start key > end key")
	}

	results := make([]KVPair, 0)

	// Find starting leaf
	n := bt.root
	for !n.isLeaf {
		i := 0
		for i < len(n.keys) && bt.comparator(startKey, n.keys[i]) >= 0 {
			i++
		}
		n = n.children[i].(*node)
	}

	// Scan from starting position
	for n != nil {
		for i := 0; i < len(n.keys); i++ {
			cmp := bt.comparator(n.keys[i], startKey)
			if cmp >= 0 {
				cmp = bt.comparator(n.keys[i], endKey)
				if cmp > 0 {
					return results, nil
				}

				keyCopy := make([]byte, len(n.keys[i]))
				copy(keyCopy, n.keys[i])
				valueCopy := make([]byte, len(n.children[i].([]byte)))
				copy(valueCopy, n.children[i].([]byte))

				results = append(results, KVPair{
					Key:   keyCopy,
					Value: valueCopy,
				})
			}
		}
		n = n.next
	}

	return results, nil
}

// KVPair represents a key-value pair.
type KVPair struct {
	Key   []byte
	Value []byte
}

// Stats returns statistics about the B+Tree.
func (bt *BTree) Stats() TreeStats {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	stats := TreeStats{
		Height: bt.height,
	}

	bt.collectStats(bt.root, &stats)
	return stats
}

// collectStats recursively collects statistics.
func (bt *BTree) collectStats(n *node, stats *TreeStats) {
	stats.TotalNodes++

	if n.isLeaf {
		// Only count keys in leaf nodes for B+Tree
		stats.TotalKeys += len(n.keys)
		stats.LeafNodes++
		if len(n.keys) > stats.MaxKeysInNode {
			stats.MaxKeysInNode = len(n.keys)
		}
		if stats.MinKeysInNode == 0 || len(n.keys) < stats.MinKeysInNode {
			stats.MinKeysInNode = len(n.keys)
		}
	} else {
		stats.InternalNodes++
		for _, child := range n.children {
			bt.collectStats(child.(*node), stats)
		}
	}
}

// TreeStats contains statistics about the B+Tree.
type TreeStats struct {
	Height        int
	TotalNodes    int
	InternalNodes int
	LeafNodes     int
	TotalKeys     int
	MaxKeysInNode int
	MinKeysInNode int
}
