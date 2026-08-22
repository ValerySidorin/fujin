package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/configurator"
)

var errConnectorRuntimeClosed = errors.New("runtime connector queue closed")

type connectorSnapshotRequest struct {
	ctx      context.Context
	snapshot configurator.ConnectorSnapshot
	result   chan configurator.ApplyResult
}

type connectorRuntimeQueue struct {
	controller *connectorRuntimeController
	ctx        context.Context
	cancel     context.CancelFunc
	wake       chan struct{}
	done       chan struct{}

	mu      sync.Mutex
	closed  bool
	pending *connectorSnapshotRequest
}

func newConnectorRuntimeQueue(
	ctx context.Context,
	controller *connectorRuntimeController,
) *connectorRuntimeQueue {
	if ctx == nil {
		ctx = context.Background()
	}
	queueCtx, cancel := context.WithCancel(ctx)
	queue := &connectorRuntimeQueue{
		controller: controller,
		ctx:        queueCtx,
		cancel:     cancel,
		wake:       make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
	go queue.run()
	return queue
}

func (q *connectorRuntimeQueue) Submit(
	ctx context.Context,
	snapshot configurator.ConnectorSnapshot,
) <-chan configurator.ApplyResult {
	if ctx == nil {
		ctx = context.Background()
	}
	request := &connectorSnapshotRequest{
		ctx:      ctx,
		snapshot: snapshot,
		result:   make(chan configurator.ApplyResult, 1),
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		completeSnapshotRequest(request, configurator.ApplyResult{
			Revision: snapshot.Revision,
			State:    configurator.ApplyRejected,
			Err:      errConnectorRuntimeClosed,
		})
		return request.result
	}
	if q.pending == nil {
		q.pending = request
	} else if snapshot.Revision > q.pending.snapshot.Revision {
		superseded := q.pending
		q.pending = request
		completeSnapshotRequest(superseded, supersededResult(
			superseded.snapshot.Revision,
			snapshot.Revision,
		))
	} else {
		pendingRevision := q.pending.snapshot.Revision
		q.mu.Unlock()
		completeSnapshotRequest(request, supersededResult(snapshot.Revision, pendingRevision))
		return request.result
	}
	q.mu.Unlock()

	select {
	case q.wake <- struct{}{}:
	default:
	}
	return request.result
}

func (q *connectorRuntimeQueue) Status() configurator.ConnectorRuntimeStatus {
	return q.controller.Status()
}

func (q *connectorRuntimeQueue) SetSourceConnected(connected bool) {
	q.controller.SetSourceConnected(connected)
}

func (q *connectorRuntimeQueue) Close() {
	q.cancel()
	<-q.done
}

func (q *connectorRuntimeQueue) run() {
	defer close(q.done)
	for {
		select {
		case <-q.ctx.Done():
			q.shutdown(q.ctx.Err())
			return
		case <-q.wake:
		}

		for {
			request := q.takePending()
			if request == nil {
				break
			}
			if err := q.ctx.Err(); err != nil {
				completeSnapshotRequest(request, configurator.ApplyResult{
					Revision: request.snapshot.Revision,
					State:    configurator.ApplyRejected,
					Err:      err,
				})
				q.shutdown(err)
				return
			}
			applyCtx, cancel := context.WithCancel(request.ctx)
			stopQueueCancel := context.AfterFunc(q.ctx, cancel)
			result := q.controller.Apply(applyCtx, request.snapshot)
			stopQueueCancel()
			cancel()
			completeSnapshotRequest(request, result)
		}
	}
}

func (q *connectorRuntimeQueue) takePending() *connectorSnapshotRequest {
	q.mu.Lock()
	defer q.mu.Unlock()
	request := q.pending
	q.pending = nil
	return request
}

func (q *connectorRuntimeQueue) shutdown(err error) {
	q.mu.Lock()
	q.closed = true
	pending := q.pending
	q.pending = nil
	q.mu.Unlock()
	if pending != nil {
		completeSnapshotRequest(pending, configurator.ApplyResult{
			Revision: pending.snapshot.Revision,
			State:    configurator.ApplyRejected,
			Err:      err,
		})
	}
}

func supersededResult(revision, newerRevision uint64) configurator.ApplyResult {
	return configurator.ApplyResult{
		Revision: revision,
		State:    configurator.ApplySuperseded,
		Err:      fmt.Errorf("connector snapshot revision %d superseded by pending revision %d", revision, newerRevision),
	}
}

func completeSnapshotRequest(request *connectorSnapshotRequest, result configurator.ApplyResult) {
	request.result <- result
	close(request.result)
}
