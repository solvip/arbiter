package pool

import (
	"errors"
	"testing"
	"time"
)

func TestEmptyPool(t *testing.T) {
	emptyPool := New()

	b, err := emptyPool.GetForRead()
	if b != nil || err != ErrNoneAvailable {
		t.Fatalf("Expected to get nil, ErrNoBackend, instead got: %v, %v", b, err)
	}

	b, err = emptyPool.GetForWrite()
	if b != nil || err != ErrNoneAvailable {
		t.Fatalf("Expected to get nil, ErrNoBackend, instead got: %v, %v", b, err)
	}
}

func TestGet(t *testing.T) {
	p := New()

	a := &mockend{state: READ_ONLY, id: "a"}
	b := &mockend{state: READ_WRITE, id: "b"}
	c := &mockend{state: UNAVAILABLE, id: "c", err: errors.New("asdf")}

	p.Put(a)
	p.Put(b)
	p.Put(c)

	time.Sleep(1100 * time.Millisecond)

	it, err := p.GetForRead()
	if err != nil {
		t.Fatalf("Expected to get a backend, instead got error: %v", err)
	}

	if it.(*mockend).id == "c" {
		t.Fatalf("GetForRead() should not have returned an UNAVAILABLE backend")
	}

	it, err = p.GetForWrite()
	if err != nil || it.(*mockend).id != b.id {
		t.Fatalf("Expected to get a primary backend, instead got: %v, %v", it, err)
	}
}

func TestFail(t *testing.T) {
	p := New()

	// Since the initial state of all backends in pool is unavailable, allow one health
	// check to succeed.
	a := &mockend{state: READ_WRITE, id: "a"}
	p.Put(a)

	time.Sleep(1001 * time.Millisecond)

	a.err = errors.New("Kill")

	time.Sleep(1001 * time.Millisecond)

	if !a.fail {
		t.Fatalf("Expected a.Fail() to have been called; a = %#v, %s", a)
	}

	if len(p.avail) != 0 {
		t.Fatalf("Expected the pool to have no available backends")
	}

	if p.primary != nil {
		t.Fatalf("Expected the pool to have no primary backend")
	}
}

type mockend struct {
	id    string
	err   error
	state State
	fail  bool
}

func (m *mockend) Ping() (State, error) {
	return m.state, m.err
}

func (m *mockend) Fail() {
	m.fail = true
}

func (m *mockend) Addr() string {
	return "foo"
}

func (m *mockend) Connect(t time.Duration) (c Conn, err error) {
	return c, err
}
