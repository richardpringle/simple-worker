use std::{
    future::{Future, Ready},
    task::Poll,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    pin,
    sync::{broadcast, mpsc},
    time::timeout,
};

#[derive(Debug, Error)]
pub enum ShutdownError {
    #[error("timed out waiting for graceful shutdown")]
    Timeout,
}

pub struct Shutdown {
    workers_complete: Complete,
    workers_notifier: CompletionNotifier,
    shutdown_sender: broadcast::Sender<ShutdownMsg>,
    timeout: Duration,
}

impl Shutdown {
    pub fn new(timeout: Duration) -> Self {
        let (workers_notifier, workers_complete) = {
            let (sender, receiver) = mpsc::channel(1);
            (CompletionNotifier(sender), Complete(receiver))
        };

        let (shutdown_sender, _shutdown_receiver) = broadcast::channel(10);

        Self {
            workers_complete,
            workers_notifier,
            shutdown_sender,
            timeout,
        }
    }

    pub fn subcribe_to_shutdown(&self) -> broadcast::Receiver<ShutdownMsg> {
        self.shutdown_sender.subscribe()
    }

    #[must_use]
    pub fn completion_notifier(&self) -> CompletionNotifier {
        self.workers_notifier.clone()
    }

    pub async fn signal_shutdown(&self, shutdown_condition: impl Future<Output = ()> + Send) {
        shutdown_condition.await;

        self.shutdown_sender
            .send(ShutdownMsg)
            .expect("failed to send shutdown signal");
    }

    pub async fn handle_shutdown(self) -> Result<(), ShutdownError> {
        // must use this function to drop the CompletionNotifier (and the rest of self)
        let (timeout_duration, complete) = self.into_timeout_info();

        // wait for the timeout duration before ending all processes
        timeout(timeout_duration, complete)
            .await
            .map_err(|_| ShutdownError::Timeout)
    }

    fn into_timeout_info(self) -> (Duration, Complete) {
        (self.timeout, self.workers_complete)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ShutdownMsg;

pub struct Complete(mpsc::Receiver<()>);

/// when the WorkerCompletionNotifier is dropped, the associated `WorkersComplete` future will complete
#[derive(Clone)]
#[must_use]
pub struct CompletionNotifier(mpsc::Sender<()>);

impl Future for Complete {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let fut = self.0.recv();
        pin!(fut);

        match fut.poll(cx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub trait Worker {
    fn setup<Shutdown: Future<Output = ()>>(
        self: Box<Self>,
        shutdown: Shutdown,
    ) -> (Box<Self>, Shutdown) {
        (self, shutdown)
    }

    fn run<Shutdown>(self: Box<Self>, shutdown: Shutdown) -> Ready<()>
    where
        Shutdown: Future<Output = ()> + Send;

    fn spawn<T>(f: T)
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static;

    fn start<Shutdown>(self: Box<Self>, shutdown: Shutdown, completion_notifier: CompletionNotifier)
    where
        Self: Send + 'static,
        Shutdown: Future<Output = ()> + Send + 'static,
    {
        let (this, shutdown) = self.setup(shutdown);

        Self::spawn(async move {
            this.run(shutdown).await;
            completion_notifier.0.send(()).await.unwrap();
        });
    }
}
