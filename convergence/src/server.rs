//! Contains utility types and functions for starting and running servers.

use crate::connection::{Connection, ConnectionError};
use crate::engine::Engine;
use crate::protocol::ProtocolError;
use std::fs::{self, File};
use std::io::{self, BufReader};
use std::pin::Pin;
use std::sync::Arc;
use rustls::{Certificate, PrivateKey, ServerConfig};
use tokio::net::{TcpListener, UnixListener};
use tokio_rustls::TlsAcceptor;

/// Controls how servers bind to local network resources.
#[derive(Default)]
pub struct BindOptions {
	addr: String,
	port: u16,
	path: Option<String>,
	certificate_path: Option<String>,
	private_key_path: Option<String>,
}

impl BindOptions {
	/// Creates a default set of options listening only on the loopback address
	/// using the default Postgres port of 5432.
	pub fn new() -> Self {
		Self {
			addr: "127.0.0.1".to_owned(),
			port: 5432,
			path: None,
			certificate_path: None,
			private_key_path: None,
		}
	}

	/// Sets the port to be used.
	pub fn with_port(mut self, port: u16) -> Self {
		self.port = port;
		self
	}

	/// Sets the address to be used.
	pub fn with_addr(mut self, addr: impl Into<String>) -> Self {
		self.addr = addr.into();
		self
	}

	pub fn with_tls(mut self, certificate_path: impl Into<String>, private_key_path: impl Into<String>) -> Self {
		self.certificate_path = Some(certificate_path.into());
		self.private_key_path = Some(private_key_path.into());
		self
	}

	pub fn with_socket_path(mut self, path: impl Into<String>) -> Self {
		self.path = Some(path.into());
		self
	}

	pub fn use_socket(&self) -> bool {
		self.path.is_some()
	}

	pub fn use_tcp(&self) -> bool {
		self.path.is_none()
	}

	pub fn use_tls(&self) -> bool {
		self.certificate_path.is_some() && self.private_key_path.is_some()
	}

	/// Configures the server to listen on all interfaces rather than any specific address.
	pub fn use_all_interfaces(self) -> Self {
		self.with_addr("0.0.0.0")
	}
}

type EngineFunc<E> = Arc<dyn Fn() -> Pin<Box<dyn futures::Future<Output = E> + Send>> + Send + Sync>;

/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Does not return unless the server terminates entirely.
pub async fn run<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	if bind.use_socket() {
		run_with_socket(bind, engine_func).await
	} else {
		// run_with_tcp(bind, engine_func).await
		run_with_tls_over_tcp(bind, engine_func).await
	}
}

/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Does not return unless the server terminates entirely.
pub async fn run_with_tcp<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	tracing::info!("Starting CipherStash on port {}", bind.port);

	let listener = TcpListener::bind((bind.addr, bind.port)).await?;

	loop {
		let (stream, _) = listener.accept().await?;

		let engine_func = engine_func.clone();
		tokio::spawn(async move {
			let mut conn = Connection::new(engine_func().await);
			conn.run(stream).await.unwrap();
		});
	}
}

/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Does not return unless the server terminates entirely.
pub async fn run_with_socket<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	let path = bind.path.unwrap();

	fs::remove_file(&path).unwrap_or_default(); // Remove the file if it already exists

	tracing::info!("Starting CipherStash on socket {}", path);

	let listener = UnixListener::bind(path).unwrap();

	loop {
		let (stream, _) = listener.accept().await?;

		let engine_func = engine_func.clone();
		tokio::spawn(async move {
			let mut conn = Connection::new(engine_func().await);

			let result = conn.run(stream).await;

			if let Err(ConnectionError::Protocol(ProtocolError::Io(e))) = result {
				//Broken Pipe - client disconnected
				tracing::error!("Connection error: {}", e);
			}
		});
	}
}



// Load certificate from file path
fn load_certs(filename: &str) -> Vec<Certificate> {
	tracing::debug!("Loading certificate from {}", filename);
	let certfile = File::open(filename).unwrap();
	let mut reader = BufReader::new(certfile);

	let certs = rustls_pemfile::certs(&mut reader).unwrap();
    certs.into_iter().map(Certificate).collect()
}

// Load private keyfrom file path
fn load_private_key(filename: &str) -> PrivateKey {
	tracing::debug!("Loading private key from {}", filename);

	let certfile = File::open(filename).unwrap();
	let mut reader = BufReader::new(certfile);

	let key = rustls_pemfile::pkcs8_private_keys(&mut reader).unwrap().remove(0);
	PrivateKey(key)
}


/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Does not return unless the server terminates entirely.
pub async fn run_with_tls_over_tcp<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	tracing::info!("Starting CipherStash on port {}", bind.port);

	let certificate_path = bind.certificate_path.unwrap();
	let private_key_path = bind.private_key_path.unwrap();

	let certs = load_certs(&certificate_path); // Replace with the path to your certificate
	let key = load_private_key(&private_key_path); // Replace with the path to your private key

	let config = ServerConfig::builder()
		.with_safe_defaults()
		.with_no_client_auth()
		.with_single_cert(certs, key)
		.map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

	let acceptor = TlsAcceptor::from(Arc::new(config));

	let listener = TcpListener::bind((bind.addr, bind.port)).await?;

	loop {
		let (stream, _) = listener.accept().await?;
        let acceptor = acceptor.clone();

		println!("TLSAcceptor");

        let engine_func = engine_func.clone();

		let future = async move {
			println!("Connection::new");
			let mut conn = Connection::new(engine_func().await);
			println!("//Connection::new");


			println!("TLSAcceptor.accept");
			let stream = acceptor.accept(stream).await?;
			println!("//TLSAcceptor.accept");


			println!("Connection.run");
			conn.run(stream).await.unwrap();
			println!("//Connection.run");

			Ok(()) as io::Result<()>
		};

		tokio::spawn(async move {
            if let Err(err) = future.await {
				println!("!!!! Error: {:?}", err);
                eprintln!("{:?}", err);
            }
        });
	}
}

