// Copyright 2022 Oxide Computer Company
use std::fs::File;
use std::io::{self, BufReader};
use std::sync::Arc;

// Reference tokio-rustls repo examples/server/src/main.rs
use rustls_pemfile::{certs, rsa_private_keys};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivatePkcs1KeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};

pub fn load_certs(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    certs(&mut BufReader::new(File::open(path)?)).collect()
}

pub fn load_rsa_keys(
    path: &str,
) -> io::Result<Vec<PrivatePkcs1KeyDer<'static>>> {
    rsa_private_keys(&mut BufReader::new(File::open(path)?)).collect()
}

#[derive(thiserror::Error, Debug)]
pub enum TLSContextError {
    #[error("IO error")]
    IOError(#[from] std::io::Error),

    #[error("rustls error")]
    RusTLSError(#[from] tokio_rustls::rustls::Error),

    #[error("client cert verifier builder error")]
    ClientVerifierBuilderError(
        #[from] tokio_rustls::rustls::client::VerifierBuilderError,
    ),
}

#[derive(Debug)]
pub struct TLSContext {
    certs: Vec<CertificateDer<'static>>,
    keys: Vec<PrivatePkcs1KeyDer<'static>>,
    root_cert_store: Arc<RootCertStore>,
}

impl TLSContext {
    pub fn from_paths(
        cert_pem_path: &str,
        key_pem_path: &str,
        root_cert_pem_path: &str,
    ) -> Result<Self, TLSContextError> {
        let mut root_cert_store = RootCertStore::empty();
        for root_cert in load_certs(root_cert_pem_path)? {
            root_cert_store.add(root_cert)?;
        }

        Ok(Self {
            certs: load_certs(cert_pem_path)?,
            keys: load_rsa_keys(key_pem_path)?,
            root_cert_store: Arc::new(root_cert_store),
        })
    }

    pub fn get_client_config(&self) -> Result<ClientConfig, TLSContextError> {
        Ok(ClientConfig::builder()
            .with_root_certificates(self.root_cert_store.clone())
            .with_client_auth_cert(
                self.certs.clone(),
                self.keys[0].clone_key().into(),
            )?)
    }

    pub fn get_server_config(&self) -> Result<ServerConfig, TLSContextError> {
        let client_cert_verifier =
            WebPkiClientVerifier::builder(self.root_cert_store.clone())
                .build()?;

        Ok(ServerConfig::builder()
            .with_client_cert_verifier(client_cert_verifier)
            .with_single_cert(
                self.certs.clone(),
                self.keys[0].clone_key().into(),
            )?)
    }
}
