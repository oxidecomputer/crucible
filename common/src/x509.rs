// Copyright 2022 Oxide Computer Company
use std::fs::File;
use std::io::{self, BufReader};
use std::sync::Arc;

// Reference tokio-rustls repo examples/server/src/main.rs
use rustls_pemfile::{certs, rsa_private_keys};
use tokio_rustls::rustls::server::AllowAnyAuthenticatedClient;
use tokio_rustls::rustls::{
    Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig,
};

pub fn load_certs(path: &str) -> io::Result<Vec<Certificate>> {
    certs(&mut BufReader::new(File::open(path)?))
        .map(|v| v.map(|c| Certificate(c.to_vec())))
        .collect::<Result<Vec<Certificate>, _>>()
        .map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid cert")
        })
}

pub fn load_rsa_keys(path: &str) -> io::Result<Vec<PrivateKey>> {
    rsa_private_keys(&mut BufReader::new(File::open(path)?))
        .map(|v| v.map(|c| PrivateKey(c.secret_pkcs1_der().to_owned())))
        .collect::<Result<Vec<PrivateKey>, _>>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"))
}

#[derive(thiserror::Error, Debug)]
pub enum TLSContextError {
    #[error("IO error")]
    IOError(#[from] std::io::Error),

    #[error("rustls error")]
    RusTLSError(#[from] tokio_rustls::rustls::Error),
}

#[derive(Debug)]
pub struct TLSContext {
    certs: Vec<Certificate>,
    keys: Vec<PrivateKey>,
    root_cert_store: RootCertStore,
}

impl TLSContext {
    pub fn from_paths(
        cert_pem_path: &str,
        key_pem_path: &str,
        root_cert_pem_path: &str,
    ) -> Result<Self, TLSContextError> {
        let mut root_cert_store = RootCertStore::empty();
        for root_cert in load_certs(root_cert_pem_path)? {
            root_cert_store.add(&root_cert)?;
        }

        Ok(Self {
            certs: load_certs(cert_pem_path)?,
            keys: load_rsa_keys(key_pem_path)?,
            root_cert_store,
        })
    }

    pub fn get_client_config(&self) -> Result<ClientConfig, TLSContextError> {
        Ok(ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(self.root_cert_store.clone())
            .with_client_auth_cert(self.certs.clone(), self.keys[0].clone())?)
    }

    pub fn get_server_config(&self) -> Result<ServerConfig, TLSContextError> {
        let client_cert_verifier =
            AllowAnyAuthenticatedClient::new(self.root_cert_store.clone());

        Ok(ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(Arc::new(client_cert_verifier))
            .with_single_cert(self.certs.clone(), self.keys[0].clone())?)
    }
}
