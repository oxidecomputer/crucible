// Copyright 2023 Oxide Computer Company
use anyhow::{bail, Result};
use dropshot::{
    ConfigDropshot, ConfigLogging, ConfigLoggingLevel, HandlerTaskMode,
};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter_producer::{Config, LogConfig, Server};
use std::net::SocketAddr;
use uuid::Uuid;

// Create the dropshot endpoint we will open to Oximeter to read stats.
// On success, return the started but not yet running Server.
pub async fn client_oximeter(
    my_address: SocketAddr,
    registration_address: SocketAddr,
) -> Result<Server> {
    println!(
        "Attempt to register {:?} with Nexus/Oximeter at {:?}",
        my_address, registration_address
    );

    let dropshot_config = ConfigDropshot {
        bind_address: my_address,
        request_body_max_bytes: 2048,
        default_handler_task_mode: HandlerTaskMode::Detached,
    };

    let logging_config = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Error,
    };

    let server_info = ProducerEndpoint {
        id: Uuid::new_v4(),
        kind: ProducerKind::Service,
        address: my_address,
        base_route: "/collect".to_string(),
        interval: tokio::time::Duration::from_secs(10),
    };

    let config = Config {
        server_info,
        registration_address,
        dropshot: dropshot_config,
        log: LogConfig::Config(logging_config),
    };

    match Server::start(&config).await {
        Ok(server) => {
            println!(
                "connected {:?} to oximeter {:?}",
                my_address, registration_address
            );
            Ok(server)
        }
        Err(e) => {
            bail!("Can't connect to oximeter server: {}", e)
        }
    }
}
