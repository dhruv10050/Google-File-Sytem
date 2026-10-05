package common

import (
	"sync"

	"google.golang.org/grpc"
)

// ConnPool caches one gRPC connection per address. GFS clients keep
// persistent connections to chunkservers (§2.5); so do chunkservers to each
// other and the master to chunkservers.
type ConnPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewConnPool() *ConnPool { return &ConnPool{conns: map[string]*grpc.ClientConn{}} }

func (p *ConnPool) Get(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = c
	return c, nil
}

func (p *ConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for a, c := range p.conns {
		c.Close()
		delete(p.conns, a)
	}
}
