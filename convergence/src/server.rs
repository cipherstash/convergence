//! Contains utility types and functions for starting and running servers.

use crate::connection::{Connection, ConnectionError};
use crate::engine::Engine;
use crate::protocol::ProtocolError;
use std::fs::{self, File};
use std::io::{self, BufReader, Read};
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::{TcpListener, UnixListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use native_tls::{TlsConnector, Identity, TlsAcceptor};


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



fn load_identity(certificate_path: String, private_key_path: String) -> Identity {
	tracing::debug!("Loading certificate from {}", certificate_path);
	let mut certificate_file = File::open(certificate_path).unwrap();

    let mut certs = vec![];
    certificate_file.read_to_end(&mut certs).unwrap();


	tracing::debug!("Loading private key from {}", private_key_path);
	let mut key_file = File::open(private_key_path).unwrap();
	let mut key = vec![];
    key_file.read_to_end(&mut key).unwrap();

    Identity::from_pkcs8(&certs, &key).unwrap()
}

/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Does not return unless the server terminates entirely.
pub async fn run_with_tls_over_tcp<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	tracing::info!("Starting CipherStash on port {}", bind.port);

	let certificate_path = bind.certificate_path.unwrap();
	let private_key_path = bind.private_key_path.unwrap();

	tracing::debug!("Load identity");
	let identity  = load_identity(certificate_path , private_key_path); // Replace with the path to your private key

	tracing::debug!("Create TcpListener");
	let listener = TcpListener::bind((bind.addr, bind.port)).await?;

	tracing::debug!("Create TlsAcceptor");
	let config = native_tls::TlsAcceptor::builder(identity).build().unwrap();
	let acceptor = tokio_native_tls::TlsAcceptor::from(config);

	loop {
		let (stream, client_addr) = listener.accept().await?;
		let acceptor = acceptor.clone();

		tracing::debug!("Accept Connection from {}", client_addr);

        let engine_func = engine_func.clone();

		tokio::spawn(async move {
			tracing::debug!("SPAWN");
			// let mut stream = acceptor.accept(stream).await.unwrap();

			// let Ok(mut stream) = acceptor.accept(stream).await else {
			// 	tracing::debug!("HELLO");
			// 	return;
			// };

			match acceptor.accept(stream).await {
                Ok(stream) => {
					println!("!!!!!!!!!!!!!!!!!!!!!!!!!");
					println!("Stream: {:?}", stream);
                },
                Err(e) => eprintln!("TLS: {}", e)
            }


			tracing::debug!("//SPAWN");
			// let mut conn = Connesction::new(engine_func().await);
			// conn.run(stream).await.unwrap();
		});

	}
}




// let future = async move {
// 	tracing::debug!("Connection::new");
// 	let mut conn = Connection::new(engine_func().await);
// 	tracing::debug!("//Connection::new");

// 	tracing::debug!("TLSAcceptor.accept");
// 	let mut stream = acceptor.accept(stream).await.expect("accept error");
// 	tracing::debug!("//TLSAcceptor.accept");

// 	tracing::debug!("Connection.run");
// 	conn.run(stream).await.unwrap();
// 	tracing::debug!("//Connection.run");

// 	Ok(()) as io::Result<()>
// };

// tokio::spawn(async move {
// 	if let Err(err) = future.await {
// 		println!("!!!! Error: {:?}", err);
// 		eprintln!("{:?}", err);
// 	}
// });