use std::{
    pin::Pin,
    task::{Context, Poll}
    ,
};

use hyper::Body;
use tonic::body::BoxBody;
use tower::{Layer, Service};

#[derive(Debug, Default, Clone)]
pub struct LoggingInterceptor<S> {
    inner: S,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct LoggingInterceptorLayer;

impl<S> Layer<S> for LoggingInterceptorLayer {
    type Service = LoggingInterceptor<S>;

    fn layer(&self, service: S) -> Self::Service {
        LoggingInterceptor { inner: service }
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output=T> + Send + 'a>>;

impl<S> Service<hyper::Request<Body>> for LoggingInterceptor<S>
    where
        S: Service<hyper::Request<Body>, Response=hyper::Response<BoxBody>> + Clone + Send + 'static,
        S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            log::info!("Incoming request: {:?}", req);
            let response = inner.call(req).await?;
            log::info!("Response: {:?}", response);
            Ok(response)
        })
    }
}
