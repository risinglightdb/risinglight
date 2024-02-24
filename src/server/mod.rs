// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

mod processor;

use std::sync::Arc;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::PlaceholderExtendedQueryHandler;
use pgwire::api::{MakeHandler, StatelessMakeHandler};
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tracing::info;

use crate::server::processor::Processor;
use crate::Database;

pub async fn run_server(host: Option<String>, port: Option<u16>, db: Database) {
    let processor = Arc::new(Processor::new(db));
    let authenticator = Arc::new(NoopStartupHandler);
    let addr = format!(
        "{}:{}",
        host.unwrap_or_else(|| "127.0.0.1".to_string()),
        port.unwrap_or(5432)
    );
    let listener = TcpListener::bind(&addr).await.unwrap();
    info!("Listening on: {}", addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.clone();
        let processor_ref = processor.clone();
        let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
            PlaceholderExtendedQueryHandler,
        )));
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref.clone(),
                placeholder.make(),
            )
            .await
        });
    }
}
