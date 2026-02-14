use tabby::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub async fn connect() -> Client<tokio_util::compat::Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr()).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    Client::connect(config, tcp.compat_write()).await.unwrap()
}

#[allow(dead_code)]
pub async fn connect_with_config(
    config: Config,
) -> Result<Client<tokio_util::compat::Compat<TcpStream>>, tabby::error::Error> {
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|e| tabby::error::Error::Io {
            kind: e.kind(),
            message: e.to_string(),
        })?;
    tcp.set_nodelay(true).unwrap();
    Client::connect(config, tcp.compat_write()).await
}
