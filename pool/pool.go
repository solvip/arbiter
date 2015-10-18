// pool implements a pool of postgres backends
package pool

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

var ErrNoneAvailable = errors.New("no backend available")

type member struct {
	b     Backend
	state State
	lat   time.Duration
}

func (m member) String() string {
	return fmt.Sprintf("member[addr: %s, state = %s, latency = %s]", m.b.Addr(), m.state, m.lat)
}

type Pool struct {
	sync.RWMutex

	// all members registered to this pool.
	members []member

	// all available members; always ordered by latency.
	avail []*member

	// Always points to the primary member.
	primary *member
}

// Return a new pool
func New() *Pool {
	return &Pool{}
}

func (p *Pool) Put(backend Backend) {
	p.Lock()
	defer p.Unlock()

	b := member{b: backend}

	p.members = append(p.members, b)
	go p.monitor(&b)
}

// Get a member; can return any - including the primary.
func (p *Pool) GetForRead() (b Backend, err error) {
	p.RLock()
	defer p.RUnlock()

	if len(p.avail) == 0 {
		return nil, ErrNoneAvailable
	}

	return p.avail[0].b, nil
}

// Get a member that's available for writes; 'always' the primary.
func (p *Pool) GetForWrite() (b Backend, err error) {
	p.RLock()
	defer p.RUnlock()

	if p.primary == nil {
		return nil, ErrNoneAvailable
	}

	return p.primary.b, nil
}

// Monitor a member
func (p *Pool) monitor(m *member) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for _ = range ticker.C {
		start := time.Now()
		newstate, err := m.b.Ping()
		lat := time.Since(start)

		p.Lock()

		switch {
		case err != nil && m.state != UNAVAILABLE:
			// We must be going down
			newstate = UNAVAILABLE
			p.avail = remove(p.avail, m)
			if m.state == READ_WRITE {
				p.primary = nil
			}
			m.b.Fail()

		case err != nil && m.state == UNAVAILABLE:
			newstate = UNAVAILABLE
			// Nothing to do.  Still down.

		case err == nil && m.state == newstate:
			// Nothing changed.

		case err == nil && m.state == UNAVAILABLE:
			// Going from unavailable to available
			p.avail = append(p.avail, m)
			if newstate == READ_WRITE {
				p.primary = m
			}

		case err == nil && m.state == READ_WRITE && newstate == READ_ONLY:
			// The member transition from primary to follower; fail all connections and
			// let client applications reconnect.
			// We could be smarter here and only fail read-write connections.
			p.primary = nil
			m.b.Fail()

		case err == nil && m.state == READ_ONLY && newstate == READ_WRITE:
			// The member transitioned from follower to primary
			p.primary = m
		}

		if m.state != newstate {
			log.Printf("%s: transitioning to %s", m, newstate)
		}

		m.state = newstate
		m.lat = lat
		sort.Sort(byLatency(p.avail))

		p.Unlock()
	}
}

type byLatency []*member

func (coll byLatency) Len() int           { return len(coll) }
func (coll byLatency) Swap(i, j int)      { coll[i], coll[j] = coll[j], coll[i] }
func (coll byLatency) Less(i, j int) bool { return coll[i].lat < coll[j].lat }

// Opposite of append.  Remove it from s, returning s - it.
func remove(s []*member, it *member) (ret []*member) {
	for _, v := range s {
		if v == it {
			continue
		}
		ret = append(ret, v)
	}

	return ret
}
