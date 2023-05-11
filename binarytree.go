package main

import "fmt"

type BinaryNode struct {
	left  *BinaryNode
	right *BinaryNode
	Value *Worker
}
type BinaryTree struct {
	root *BinaryNode
}

func NewBinaryTree() *BinaryTree {
	return &BinaryTree{
		root: &BinaryNode{Value: &Worker{Name: "root", RangeBegin: ^uint64(1 << 63), RangeEnd: uint64(1 << 63)}},
	}
}

func (t *BinaryTree) Insert(w *Worker) error {
	if t.root == nil {
		t.root = &BinaryNode{Value: w}
	}
	return t.root.insert(w)
}

func (n *BinaryNode) insert(w *Worker) error {
	if n == nil {
		return nil
	} else if w.RangeBegin < n.Value.RangeBegin && w.RangeEnd < n.Value.RangeEnd {
		if n.left == nil {
			n.left = &BinaryNode{Value: w}
		} else {
			return n.left.insert(w)
		}
	} else if w.RangeEnd > n.Value.RangeEnd {
		if n.right == nil {
			n.right = &BinaryNode{Value: w}
		} else {
			return n.right.insert(w)
		}
	} else {
		return fmt.Errorf("Worker %s already exists", w.Name)
	}
	return nil
}

func (t *BinaryTree) Delete(worker *Worker) {
	t.root.delete(worker)
}

func (n *BinaryNode) delete(w *Worker) bool {
	if n == nil {
		return false
	} else if w.RangeBegin < n.Value.RangeBegin && w.RangeEnd < n.Value.RangeEnd {
		if n.left.delete(w) {
			n.left = nil
		}
	} else if w.RangeEnd > n.Value.RangeEnd {
		if n.right.delete(w) {
			n.right = nil
		}
	} else {
		return true
	}
	return false
}

func (t *BinaryTree) Find(id uint64) *BinaryNode {
	return t.root.find(id)
}

func (n *BinaryNode) find(id uint64) *BinaryNode {
	if n == nil {
		return nil
	} else if id < n.Value.RangeBegin && id < n.Value.RangeEnd {
		return n.left.find(id)
	} else if id > n.Value.RangeEnd {
		return n.right.find(id)
	} else {
		return n
	}
}

func (t *BinaryTree) Print() {
	t.root.print(0, "Root: ")
}

func (n *BinaryNode) print(q int, prefix string) {
	if n == nil {
		return
	}
	for i := 0; i < q; i++ {
		fmt.Print(" ")
	}
	fmt.Printf("%s%x-%x | %s\n", prefix, n.Value.RangeBegin, n.Value.RangeEnd, n.Value.Name)
	n.left.print(q+2, "L: ")

	n.right.print(q+2, "R: ")
}
