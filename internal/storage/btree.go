package storage

import (
	"fmt"
	"sync"
)

const defaultOrder = 4

type BTree struct {
	root  *bTreeNode
	order int
	mu    sync.RWMutex
}

type bTreeNode struct {
	keys     []Value
	children []*bTreeNode
	isLeaf   bool
	rowPtrs  []int
}

func NewBTree() *BTree {
	return &BTree{
		root: &bTreeNode{
			keys:     make([]Value, 0),
			children: make([]*bTreeNode, 0),
			isLeaf:   true,
			rowPtrs:  make([]int, 0),
		},
		order: defaultOrder,
	}
}

func (bt *BTree) Insert(key Value, rowPtr int) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.root == nil {
		bt.root = &bTreeNode{
			keys:     []Value{key},
			children: make([]*bTreeNode, 0),
			isLeaf:   true,
			rowPtrs:  []int{rowPtr},
		}
		return nil
	}

	if len(bt.root.keys) >= 2*bt.order-1 {
		newRoot := &bTreeNode{
			keys:     make([]Value, 0),
			children: []*bTreeNode{bt.root},
			isLeaf:   false,
			rowPtrs:  make([]int, 0),
		}
		bt.splitChild(newRoot, 0)
		bt.insertNonFull(newRoot, key, rowPtr)
		bt.root = newRoot
	} else {
		bt.insertNonFull(bt.root, key, rowPtr)
	}

	return nil
}

func (bt *BTree) splitChild(parent *bTreeNode, i int) {
	order := bt.order
	t := parent.children[i]
	newNode := &bTreeNode{
		keys:     make([]Value, 0),
		children: make([]*bTreeNode, 0),
		isLeaf:   t.isLeaf,
		rowPtrs:  make([]int, 0),
	}

	midKey := t.keys[order-1]

	newNode.keys = append(newNode.keys, t.keys[order:]...)
	if t.isLeaf {
		newNode.rowPtrs = append(newNode.rowPtrs, t.rowPtrs[order:]...)
	} else {
		newNode.children = append(newNode.children, t.children[order:]...)
	}

	t.keys = t.keys[:order-1]
	if t.isLeaf {
		t.rowPtrs = t.rowPtrs[:order-1]
	} else {
		t.children = t.children[:order]
	}

	parent.keys = append(parent.keys[:i], append([]Value{midKey}, parent.keys[i:]...)...)
	parent.children = append(parent.children[:i+1], append([]*bTreeNode{newNode}, parent.children[i+1:]...)...)
}

func (bt *BTree) insertNonFull(node *bTreeNode, key Value, rowPtr int) {
	i := len(node.keys) - 1

	if node.isLeaf {
		node.keys = append(node.keys, nil)
		node.rowPtrs = append(node.rowPtrs, 0)

		for i >= 0 && key.LessThan(node.keys[i]) {
			node.keys[i+1] = node.keys[i]
			node.rowPtrs[i+1] = node.rowPtrs[i]
			i--
		}

		node.keys[i+1] = key
		node.rowPtrs[i+1] = rowPtr
	} else {
		for i >= 0 && key.LessThan(node.keys[i]) {
			i--
		}

		i++

		if len(node.children[i].keys) >= 2*bt.order-1 {
			bt.splitChild(node, i)
			if key.Equals(node.keys[i]) {
				return
			}
			if key.LessThan(node.keys[i]) {
				i++
			}
		}

		bt.insertNonFull(node.children[i], key, rowPtr)
	}
}

func (bt *BTree) Lookup(key Value) ([]int, bool) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	return bt.lookup(bt.root, key)
}

func (bt *BTree) lookup(node *bTreeNode, key Value) ([]int, bool) {
	if node == nil {
		return nil, false
	}

	i := 0
	for i < len(node.keys) && key.LessThan(node.keys[i]) {
		i++
	}

	if i < len(node.keys) && key.Equals(node.keys[i]) {
		return []int{node.rowPtrs[i]}, true
	}

	if node.isLeaf {
		return nil, false
	}

	return bt.lookup(node.children[i], key)
}

func (bt *BTree) Range(start, end Value) []int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	result := make([]int, 0)
	bt.rangeSearch(bt.root, start, end, &result)
	return result
}

func (bt *BTree) rangeSearch(node *bTreeNode, start, end Value, result *[]int) {
	if node == nil {
		return
	}

	for i := 0; i < len(node.keys); i++ {
		if !node.isLeaf {
			bt.rangeSearch(node.children[i], start, end, result)
		}

		if node.keys[i].LessThan(end) || node.keys[i].Equals(end) {
			if start.LessThan(node.keys[i]) || start.Equals(node.keys[i]) {
				*result = append(*result, node.rowPtrs[i])
			}
		}
	}

	if !node.isLeaf {
		bt.rangeSearch(node.children[len(node.keys)], start, end, result)
	}
}

func (bt *BTree) Delete(key Value) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if !bt.containsKey(bt.root, key) {
		return fmt.Errorf("key not found")
	}

	bt.deleteKey(bt.root, key)

	if len(bt.root.keys) == 0 && !bt.root.isLeaf {
		bt.root = bt.root.children[0]
	}

	return nil
}

func (bt *BTree) containsKey(node *bTreeNode, key Value) bool {
	if node == nil {
		return false
	}

	i := 0
	for i < len(node.keys) && key.LessThan(node.keys[i]) {
		i++
	}

	if i < len(node.keys) && key.Equals(node.keys[i]) {
		return true
	}

	if node.isLeaf {
		return false
	}

	return bt.containsKey(node.children[i], key)
}

func (bt *BTree) deleteKey(node *bTreeNode, key Value) bool {
	idx := bt.findKey(node, key)

	if idx < len(node.keys) && key.Equals(node.keys[idx]) {
		if node.isLeaf {
			bt.removeFromLeaf(node, idx)
		} else {
			bt.removeFromInternal(node, idx)
		}
		return true
	}

	if node.isLeaf {
		return false
	}

	flag := idx == len(node.keys)

	if len(node.children[idx].keys) < bt.order {
		bt.fill(node, idx)
	}

	if flag && idx > len(node.keys) {
		return bt.deleteKey(node.children[idx-1], key)
	}

	return bt.deleteKey(node.children[idx], key)
}

func (bt *BTree) findKey(node *bTreeNode, key Value) int {
	idx := 0
	for idx < len(node.keys) && !key.LessThan(node.keys[idx]) {
		idx++
	}
	return idx
}

func (bt *BTree) removeFromLeaf(node *bTreeNode, idx int) {
	node.keys = append(node.keys[:idx], node.keys[idx+1:]...)
	node.rowPtrs = append(node.rowPtrs[:idx], node.rowPtrs[idx+1:]...)
}

func (bt *BTree) removeFromInternal(node *bTreeNode, idx int) {
	key := node.keys[idx]

	if len(node.children[idx].keys) >= bt.order {
		pred := bt.getPredecessor(node, idx)
		node.keys[idx] = pred
		bt.deleteKey(node.children[idx], pred)
	} else if len(node.children[idx+1].keys) >= bt.order {
		succ := bt.getSuccessor(node, idx)
		node.keys[idx] = succ
		bt.deleteKey(node.children[idx+1], succ)
	} else {
		bt.merge(node, idx)
		bt.deleteKey(node.children[idx], key)
	}
}

func (bt *BTree) getPredecessor(node *bTreeNode, idx int) Value {
	current := node.children[idx]
	for !current.isLeaf {
		current = current.children[len(current.keys)]
	}
	return current.keys[len(current.keys)-1]
}

func (bt *BTree) getSuccessor(node *bTreeNode, idx int) Value {
	current := node.children[idx+1]
	for !current.isLeaf {
		current = current.children[0]
	}
	return current.keys[0]
}

func (bt *BTree) fill(node *bTreeNode, idx int) {
	if idx != 0 && len(node.children[idx-1].keys) >= bt.order {
		bt.borrowFromPrev(node, idx)
	} else if idx != len(node.keys) && len(node.children[idx+1].keys) >= bt.order {
		bt.borrowFromNext(node, idx)
	} else {
		if idx != len(node.keys) {
			bt.merge(node, idx)
		} else {
			bt.merge(node, idx-1)
		}
	}
}

func (bt *BTree) borrowFromPrev(node *bTreeNode, idx int) {
	child := node.children[idx]
	sibling := node.children[idx-1]

	child.keys = append([]Value{node.keys[idx-1]}, child.keys...)
	child.rowPtrs = append([]int{child.rowPtrs[0]}, child.rowPtrs...)

	if !child.isLeaf {
		child.children = append([]*bTreeNode{sibling.children[len(sibling.children)-1]}, child.children...)
		sibling.children = sibling.children[:len(sibling.children)-1]
	}

	node.keys[idx-1] = sibling.keys[len(sibling.keys)-1]

	sibling.keys = sibling.keys[:len(sibling.keys)-1]
	sibling.rowPtrs = sibling.rowPtrs[:len(sibling.rowPtrs)-1]
}

func (bt *BTree) borrowFromNext(node *bTreeNode, idx int) {
	child := node.children[idx]
	sibling := node.children[idx+1]

	child.keys = append(child.keys, node.keys[idx])
	child.rowPtrs = append(child.rowPtrs, sibling.rowPtrs[0])

	if !child.isLeaf {
		child.children = append(child.children, sibling.children[0])
		sibling.children = sibling.children[1:]
	}

	node.keys[idx] = sibling.keys[0]

	sibling.keys = sibling.keys[1:]
	sibling.rowPtrs = sibling.rowPtrs[1:]
}

func (bt *BTree) merge(node *bTreeNode, idx int) {
	child := node.children[idx]
	sibling := node.children[idx+1]

	child.keys = append(child.keys, node.keys[idx])
	child.keys = append(child.keys, sibling.keys...)
	child.rowPtrs = append(child.rowPtrs, sibling.rowPtrs...)

	if !child.isLeaf {
		child.children = append(child.children, sibling.children...)
	}

	node.keys = append(node.keys[:idx], node.keys[idx+1:]...)
	node.children = append(node.children[:idx+1], node.children[idx+2:]...)
}

func (bt *BTree) ScanAll() []int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	result := make([]int, 0)
	bt.scan(bt.root, &result)
	return result
}

func (bt *BTree) scan(node *bTreeNode, result *[]int) {
	if node == nil {
		return
	}

	for i := 0; i < len(node.keys); i++ {
		if !node.isLeaf {
			bt.scan(node.children[i], result)
		}
		*result = append(*result, node.rowPtrs[i])
	}

	if !node.isLeaf {
		bt.scan(node.children[len(node.keys)], result)
	}
}

func (bt *BTree) Count() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	count := 0
	bt.count(bt.root, &count)
	return count
}

func (bt *BTree) count(node *bTreeNode, cnt *int) {
	if node == nil {
		return
	}

	for i := 0; i < len(node.keys); i++ {
		if !node.isLeaf {
			bt.count(node.children[i], cnt)
		}
		*cnt++
	}

	if !node.isLeaf {
		bt.count(node.children[len(node.keys)], cnt)
	}
}

func (bt *BTree) Dump() string {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	return bt.dumpNode(bt.root, 0)
}

func (bt *BTree) dumpNode(node *bTreeNode, depth int) string {
	if node == nil {
		return ""
	}

	result := ""
	for i := 0; i < depth; i++ {
		result += "  "
	}

	keys := make([]string, len(node.keys))
	for i, k := range node.keys {
		keys[i] = k.ToString()
	}
	result += fmt.Sprintf("Keys: %v (Leaf: %v)\n", keys, node.isLeaf)

	if !node.isLeaf {
		for _, child := range node.children {
			result += bt.dumpNode(child, depth+1)
		}
	}

	return result
}

type Index interface {
	Insert(key Value, ptr int) error
	Delete(key Value) error
	Lookup(key Value) ([]int, bool)
	Range(start, end Value) []int
	ScanAll() []int
	Count() int
}

func NewIndex() Index {
	return NewBTree()
}
