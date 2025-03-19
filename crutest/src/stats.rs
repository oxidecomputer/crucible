// Copyright 2024 Oxide Computer Company

use anyhow::{bail, Result};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter_producer::{
    Config, ConfigLogging, ConfigLoggingLevel, LogConfig, Server,
};
use std::net::SocketAddr;
use uuid::Uuid;

// Create the dropshot endpoint we will open to Oximeter to read stats.
// On success, return the running Server.
pub fn client_oximeter(
    my_address: SocketAddr,
    registration_address: SocketAddr,
) -> Result<Server> {
    println!(
        "Attempt to register {:?} with Nexus/Oximeter at {:?}",
        my_address, registration_address
    );

    let logging_config = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Error,
    };

    let server_info = ProducerEndpoint {
        id: Uuid::new_v4(),
        kind: ProducerKind::Service,
        address: my_address,
        interval: tokio::time::Duration::from_secs(10),
    };

    let config = Config {
        server_info,
        registration_address: Some(registration_address),
        default_request_body_max_bytes: 2048,
        log: LogConfig::Config(logging_config),
    };

    match Server::start(&config) {
        Ok(server) => {
            println!(
                "registered with nexus at {:?}, serving metrics to \
                oximeter from {:?}",
                registration_address, my_address,
            );
            Ok(server)
        }
        Err(e) => {
            bail!("Failed to register as metric producer with Nexus: {}", e)
        }
    }
}
