package docker

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// closeCountingClient wraps a DockerClient and records Close invocations.
type closeCountingClient struct {
	DockerClient
	mu    sync.Mutex
	calls int
	err   error
}

func (c *closeCountingClient) Close() error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.err
}

func (c *closeCountingClient) closeCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestProviderCloseIsIdempotent(t *testing.T) {
	underlying := newMockDockerClient()
	closeErr := errors.New("kaboom")
	cc := &closeCountingClient{DockerClient: underlying, err: closeErr}

	p := &Provider{client: cc}

	err1 := p.Close()
	err2 := p.Close()
	err3 := p.Close()

	require.Error(t, err1)
	assert.ErrorIs(t, err1, closeErr)
	// Every caller observes the same result and the client closes once.
	assert.Same(t, err1, err2)
	assert.Same(t, err1, err3)
	assert.Equal(t, 1, cc.closeCalls())
}

func TestProviderCloseConcurrentClosesClientOnce(t *testing.T) {
	underlying := newMockDockerClient()
	cc := &closeCountingClient{DockerClient: underlying}
	p := &Provider{client: cc}

	const callers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			errs[idx] = p.Close()
		}(i)
	}
	close(start)
	wg.Wait()

	assert.Equal(t, 1, cc.closeCalls(), "client must be closed exactly once")
	for i, err := range errs {
		assert.NoError(t, err, "caller %d", i)
	}
}

func TestProviderCloseWithoutClient(t *testing.T) {
	p := &Provider{}
	assert.NoError(t, p.Close())
	assert.NoError(t, p.Close())
}
