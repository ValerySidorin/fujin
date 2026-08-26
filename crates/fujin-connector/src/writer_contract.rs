use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use parking_lot::Mutex;

use crate::{Completion, CompletionSink, CoreError, Message, OperationToken, Result, Writer};

struct PendingOperation {
    sequence: u64,
    original: OperationToken,
    flush_snapshot: Option<u64>,
    result: Option<Result<()>>,
}

#[derive(Default)]
struct ContractInner {
    closed: bool,
    completed_through: u64,
    completed: BTreeSet<u64>,
    pending: BTreeMap<OperationToken, PendingOperation>,
}

pub(crate) struct ContractSink {
    target: Arc<dyn CompletionSink>,
    enabled: AtomicBool,
    next_token: AtomicU64,
    next_sequence: AtomicU64,
    inner: Mutex<ContractInner>,
}

impl ContractSink {
    fn new(target: Arc<dyn CompletionSink>) -> Arc<Self> {
        Arc::new(Self {
            target,
            enabled: AtomicBool::new(false),
            next_token: AtomicU64::new(1),
            next_sequence: AtomicU64::new(1),
            inner: Mutex::new(ContractInner::default()),
        })
    }

    fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    fn accept(&self, original: OperationToken, flush: bool) -> Result<OperationToken> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let mapped = OperationToken::internal(self.next_token.fetch_add(1, Ordering::Relaxed));
        let mut inner = self.inner.lock();
        if inner.closed {
            return Err(CoreError::Closed);
        }
        inner.pending.insert(
            mapped,
            PendingOperation {
                sequence,
                original,
                flush_snapshot: flush.then_some(sequence.saturating_sub(1)),
                result: None,
            },
        );
        Ok(mapped)
    }

    fn reject(&self, mapped: OperationToken) {
        let forwards = {
            let mut inner = self.inner.lock();
            let Some(operation) = inner.pending.remove(&mapped) else {
                return;
            };
            mark_completed(&mut inner, operation.sequence);
            take_ready_flushes(&mut inner)
        };
        self.forward(forwards);
    }

    fn close_pending(&self) -> Vec<Completion> {
        let pending = {
            let mut inner = self.inner.lock();
            if inner.closed {
                return Vec::new();
            }
            inner.closed = true;
            std::mem::take(&mut inner.pending)
        };
        pending
            .into_values()
            .map(|operation| Completion {
                token: operation.original,
                result: Err(CoreError::Closed),
            })
            .collect()
    }

    fn forward(&self, completions: Vec<Completion>) {
        for completion in completions {
            self.target.complete(completion);
        }
    }
}

impl CompletionSink for ContractSink {
    fn complete(&self, completion: Completion) {
        if !self.enabled.load(Ordering::Acquire) {
            self.target.complete(completion);
            return;
        }
        let forwards = {
            let mut inner = self.inner.lock();
            let Some(mut operation) = inner.pending.remove(&completion.token) else {
                return;
            };
            if operation.flush_snapshot.is_some() {
                operation.result = Some(completion.result);
                inner.pending.insert(completion.token, operation);
                take_ready_flushes(&mut inner)
            } else {
                mark_completed(&mut inner, operation.sequence);
                let mut forwards = vec![Completion {
                    token: operation.original,
                    result: completion.result,
                }];
                forwards.extend(take_ready_flushes(&mut inner));
                forwards
            }
        };
        self.forward(forwards);
    }
}

fn mark_completed(inner: &mut ContractInner, sequence: u64) {
    inner.completed.insert(sequence);
    while inner.completed.remove(&(inner.completed_through + 1)) {
        inner.completed_through += 1;
    }
}

fn take_ready_flushes(inner: &mut ContractInner) -> Vec<Completion> {
    let ready: Vec<_> = inner
        .pending
        .iter()
        .filter_map(|(token, operation)| {
            operation
                .flush_snapshot
                .filter(|snapshot| {
                    operation.result.is_some() && *snapshot <= inner.completed_through
                })
                .map(|_| *token)
        })
        .collect();
    let mut forwards = Vec::with_capacity(ready.len());
    for token in ready {
        let operation = inner.pending.remove(&token).expect("flush exists");
        mark_completed(inner, operation.sequence);
        forwards.push(Completion {
            token: operation.original,
            result: operation.result.expect("flush result exists"),
        });
    }
    forwards
}

struct ContractWriter {
    writer: Arc<dyn Writer>,
    sink: Arc<ContractSink>,
}

impl ContractWriter {
    fn submit(
        &self,
        original: OperationToken,
        flush: bool,
        operation: impl FnOnce(OperationToken) -> Result<()>,
    ) -> Result<()> {
        let mapped = self.sink.accept(original, flush)?;
        if let Err(error) = operation(mapped) {
            self.sink.reject(mapped);
            return Err(error);
        }
        Ok(())
    }
}

impl Writer for ContractWriter {
    fn produce(&self, token: OperationToken, message: Message) -> Result<()> {
        self.submit(token, false, |mapped| self.writer.produce(mapped, message))
    }

    fn flush(&self, token: OperationToken) -> Result<()> {
        self.submit(token, true, |mapped| self.writer.flush(mapped))
    }

    fn begin_transaction(&self, token: OperationToken) -> Result<()> {
        self.submit(token, false, |mapped| self.writer.begin_transaction(mapped))
    }

    fn commit_transaction(&self, token: OperationToken) -> Result<()> {
        self.submit(token, false, |mapped| {
            self.writer.commit_transaction(mapped)
        })
    }

    fn rollback_transaction(&self, token: OperationToken) -> Result<()> {
        self.submit(token, false, |mapped| {
            self.writer.rollback_transaction(mapped)
        })
    }

    fn close(self: Arc<Self>) -> crate::BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.sink.forward(self.sink.close_pending());
            Arc::clone(&self.writer).close().await
        })
    }

    fn writer_contract_compliant(&self) -> bool {
        true
    }
}

pub(crate) fn enforce_writer_contract(
    writer: Arc<dyn Writer>,
    sink: Arc<ContractSink>,
) -> Arc<dyn Writer> {
    if writer.writer_contract_compliant() {
        return writer;
    }
    sink.enable();
    Arc::new(ContractWriter { writer, sink })
}

pub(crate) fn contract_sink(target: Arc<dyn CompletionSink>) -> Arc<ContractSink> {
    ContractSink::new(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<Completion>>);

    impl CompletionSink for Recorder {
        fn complete(&self, completion: Completion) {
            self.0.lock().push(completion);
        }
    }

    struct DeferredWriter {
        sink: Arc<dyn CompletionSink>,
        pending: Mutex<Vec<OperationToken>>,
    }

    impl DeferredWriter {
        fn complete(&self, index: usize, result: Result<()>) {
            let token = self.pending.lock()[index];
            self.sink.complete(Completion { token, result });
        }
    }

    impl Writer for DeferredWriter {
        fn produce(&self, token: OperationToken, _message: Message) -> Result<()> {
            self.pending.lock().push(token);
            Ok(())
        }

        fn flush(&self, token: OperationToken) -> Result<()> {
            self.pending.lock().push(token);
            Ok(())
        }

        fn begin_transaction(&self, token: OperationToken) -> Result<()> {
            self.pending.lock().push(token);
            Ok(())
        }

        fn commit_transaction(&self, token: OperationToken) -> Result<()> {
            self.pending.lock().push(token);
            Ok(())
        }

        fn rollback_transaction(&self, token: OperationToken) -> Result<()> {
            self.pending.lock().push(token);
            Ok(())
        }

        fn close(self: Arc<Self>) -> crate::BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn token(value: u64) -> OperationToken {
        OperationToken::external(value).expect("external token")
    }

    #[test]
    fn duplicate_completion_is_forwarded_once() {
        let recorder = Arc::new(Recorder::default());
        let sink = contract_sink(Arc::clone(&recorder) as Arc<dyn CompletionSink>);
        let deferred = Arc::new(DeferredWriter {
            sink: Arc::clone(&sink) as Arc<dyn CompletionSink>,
            pending: Mutex::new(Vec::new()),
        });
        let writer = enforce_writer_contract(Arc::clone(&deferred) as Arc<dyn Writer>, sink);
        writer
            .produce(token(7), Message::new(bytes::Bytes::new()))
            .expect("accept produce");

        deferred.complete(0, Ok(()));
        deferred.complete(0, Ok(()));

        assert_eq!(recorder.0.lock().len(), 1);
        assert_eq!(recorder.0.lock()[0].token, token(7));
    }

    #[test]
    fn flush_waits_for_earlier_completions() {
        let recorder = Arc::new(Recorder::default());
        let sink = contract_sink(Arc::clone(&recorder) as Arc<dyn CompletionSink>);
        let deferred = Arc::new(DeferredWriter {
            sink: Arc::clone(&sink) as Arc<dyn CompletionSink>,
            pending: Mutex::new(Vec::new()),
        });
        let writer = enforce_writer_contract(Arc::clone(&deferred) as Arc<dyn Writer>, sink);
        writer
            .produce(token(1), Message::new(bytes::Bytes::new()))
            .expect("accept first");
        writer
            .produce(token(2), Message::new(bytes::Bytes::new()))
            .expect("accept second");
        writer.flush(token(3)).expect("accept flush");

        deferred.complete(2, Ok(()));
        deferred.complete(1, Ok(()));
        assert!(
            recorder
                .0
                .lock()
                .iter()
                .all(|value| value.token != token(3))
        );
        deferred.complete(0, Ok(()));
        assert_eq!(
            recorder.0.lock().last().expect("flush completion").token,
            token(3)
        );
    }

    #[tokio::test]
    async fn close_resolves_pending_completion() {
        let recorder = Arc::new(Recorder::default());
        let sink = contract_sink(Arc::clone(&recorder) as Arc<dyn CompletionSink>);
        let deferred = Arc::new(DeferredWriter {
            sink: Arc::clone(&sink) as Arc<dyn CompletionSink>,
            pending: Mutex::new(Vec::new()),
        });
        let writer = enforce_writer_contract(Arc::clone(&deferred) as Arc<dyn Writer>, sink);
        writer
            .produce(token(9), Message::new(bytes::Bytes::new()))
            .expect("accept produce");

        writer.close().await.expect("close writer");

        let values = recorder.0.lock();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].token, token(9));
        assert_eq!(values[0].result, Err(CoreError::Closed));
    }
}
