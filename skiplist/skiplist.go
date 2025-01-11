// Contains a simple implementation of a skip list.
package skiplist

import (
	"bytes"
	"fmt"
	"github.com/mwildt/goodb/base"
	"golang.org/x/exp/constraints"
	"log"
	"math/rand"
)

func randomLevel(maxLevel int) int {
	level := 1
	for rand.Float64() < 0.5 && level < maxLevel {
		level++
	}
	return level
}

type skipListNode[K constraints.Ordered, V any] struct {
	key   K
	value V
	next  []*skipListNode[K, V]
}

func (node *skipListNode[K, V]) follower() *skipListNode[K, V] {
	return node.next[0]
}

func (node *skipListNode[K, V]) hasLevel(level int) bool {
	return len(node.next) > level
}

func (node *skipListNode[K, V]) hasNext(level int) bool {
	return node.hasLevel(level) && node.next[level] != nil
}

type SkipList[K constraints.Ordered, V any] struct {
	head  *skipListNode[K, V]
	size  int
	level int
}

func NewSkipList[K constraints.Ordered, V any]() *SkipList[K, V] {
	return &SkipList[K, V]{
		head:  nil,
		level: 1,
	}
}

// [L3] [05 -> 30] --> --> -> --> --> --> -> --> --> --> -> --> --> [30 -> 80] --> [80 -> NL] --> NIL
// [L2] [05 -> 30] --> --> -> --> --> --> -> --> --> --> -> --> --> [30 -> 80] --> [80 -> NL] --> NIL
// [L1] [05 -> 10] --> [10 -> 30] --> --> -> --> --> --> -> --> --> [30 -> 80] --> [80 -> NL] --> NIL
// [L0] [05 -> 10] --> [10 -> 15] --> [15 -> 20] --> [20 -> 30] --> [30 -> 80] --> [80 -> NL] --> NIL

// idee:
func (sl *SkipList[K, V]) search(key K) (node *skipListNode[K, V], refs []*skipListNode[K, V]) {
	node = sl.head
	if sl.head == nil || node.key >= key {
		return node, refs
	}
	refs = make([]*skipListNode[K, V], sl.level)
	for level := sl.level - 1; level >= 0; level-- {
		for node.hasNext(level) && node.next[level].key < key {
			node = node.next[level]
		}
		refs[level] = node
	}
	return node.next[0], refs
}

func (sl *SkipList[K, V]) autoadjustLevel() {
	if sl.Size() >= 2<<(sl.level) {
		sl.increaseLevel()
	} else if sl.level > 1 && sl.Size() <= 2<<(sl.level-2) {
		sl.decreaseLevel()
	}
}

func (sl *SkipList[K, V]) increaseLevel() {
	sl.level++
	if sl.head != nil {
		sl.head.next = append(sl.head.next, nil)
	}
}

func (sl *SkipList[K, V]) decreaseLevel() {
	if sl.level <= 2 {
		return
	}
	sl.level--
	current := sl.head
	for current != nil {
		follower := current.next[sl.level]
		current.next = current.next[:sl.level]
		current = follower
	}
}

func (sl *SkipList[K, V]) newRandomNode(key K, value V) *skipListNode[K, V] {
	return &skipListNode[K, V]{
		key:   key,
		value: value,
		next:  make([]*skipListNode[K, V], randomLevel(sl.level))}
}

func (sl *SkipList[K, V]) Get(key K) (value V, found bool) {
	if sl.head == nil {
		return value, false
	}
	node, _ := sl.search(key)
	if node != nil {
		return node.value, node.key == key
	} else {
		return value, false
	}
}

func (sl *SkipList[K, V]) Set(key K, value V) {

	if sl.head == nil {
		sl.head = &skipListNode[K, V]{key, value, make([]*skipListNode[K, V], sl.level)}
		sl.size++
		return
	}
	node, refs := sl.search(key)

	if node != nil && node.key == key { // update
		node.value = value
		return
	}

	if node == sl.head { // append first
		newHead := &skipListNode[K, V]{key, value, make([]*skipListNode[K, V], sl.level)}
		newSecond := sl.newRandomNode(sl.head.key, sl.head.value)

		for level, _ := range newHead.next {
			if newSecond.hasLevel(level) {
				newHead.next[level] = newSecond
				newSecond.next[level] = node.next[level]
			} else {
				newHead.next[level] = node.next[level]
			}
		}
		sl.head = newHead
	} else if node == nil { // append end
		newNode := sl.newRandomNode(key, value)
		for level, ref := range refs[0:len(newNode.next)] {
			ref.next[level] = newNode
		}
	} else if node != nil { // append mid
		newNode := sl.newRandomNode(key, value)
		for level, ref := range refs[0:len(newNode.next)] {
			newNode.next[level] = ref.next[level]
			ref.next[level] = newNode
		}
	}
	sl.size++
	sl.autoadjustLevel()
}

func (sl *SkipList[K, V]) Delete(key K) bool {
	if sl.head == nil {
		return false
	}

	node, refs := sl.search(key)
	if node == nil || node.key != key {
		return false
	}

	// ist es das erste element?
	if node == sl.head {
		if node.follower() != nil { // erstes aber nicht einziges
			newHead := &skipListNode[K, V]{node.follower().key, node.follower().value, make([]*skipListNode[K, V], sl.level)}
			for level, _ := range newHead.next {
				if node.follower().hasLevel(level) {
					newHead.next[level] = node.follower().next[level]
				} else {
					newHead.next[level] = node.next[level]
				}
			}
			sl.head = newHead
			sl.size--
			sl.autoadjustLevel()
			return true
		} else { // erstes und einziges
			sl.head = nil
			sl.size--
			sl.autoadjustLevel()
			return true
		}
	}
	// oder es ist ein element in der mitte

	// jetzt müsen nur noch die referenten auf die referenzen der node geändert werden
	for level, ref := range refs {
		if ref.next[level] == node {
			ref.next[level] = node.next[level]
		}
	}
	sl.size--
	sl.autoadjustLevel()
	return true
}

func (sl *SkipList[K, V]) Entries() (result []base.Entry[K, V]) {
	current := sl.head
	for current != nil {
		result = append(result, base.Entry[K, V]{current.key, current.value})
		current = current.next[0]
	}
	return result
}

func (sl *SkipList[K, V]) Keys() <-chan K {
	ch := make(chan K)
	go func() {
		current := sl.head
		for current != nil {
			ch <- current.key
			current = current.next[0]
		}
		close(ch)
	}()
	return ch
}

func (sl *SkipList[K, V]) Values() <-chan V {
	ch := make(chan V)
	go func() {
		current := sl.head
		for current != nil {
			ch <- current.value
			current = current.next[0]
		}
		close(ch)
	}()
	return ch
}

func (sl *SkipList[K, V]) Size() int {
	return sl.size
}

func (sl *SkipList[K, V]) checkNodes() {
	currentNode := sl.head
	//var passedNode *skipListNode[K,V]
	lines := make([]bytes.Buffer, sl.level)

	printNode := func(node *skipListNode[K, V], level int) string {
		if level >= len(currentNode.next) {
			return "--> -> -->"
		} else if currentNode.next[level] == nil {
			return fmt.Sprintf("[%02v -> NL]", currentNode.key)
		} else {
			return fmt.Sprintf("[%02v -> %02v]", currentNode.key, currentNode.next[level].key)
		}
	}

	for currentNode != nil {

		for currentLevel := sl.level - 1; currentLevel >= 0; currentLevel-- {
			lines[currentLevel].WriteString(fmt.Sprintf("%s --> ", printNode(currentNode, currentLevel)))
		}

		if currentNode.follower() != nil && currentNode.key >= currentNode.follower().key {
			log.Printf("order nicht korrekt %v ==> %v", currentNode.key, currentNode.follower().key)
		}

		if currentNode.follower() == currentNode {
			log.Printf("we have a ring here %v", currentNode)
		}

		//passedNode = currentNode
		currentNode = currentNode.next[0]

	}

	for currentLevel := sl.level - 1; currentLevel >= 0; currentLevel-- {
		fmt.Printf(" [L%d] %sNIL\n", currentLevel, lines[currentLevel].String())
	}

}
