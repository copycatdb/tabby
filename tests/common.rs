use tabby::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub async fn connect() -> Client<tokio_util::compat::Compat<TcpStream>> {
    let host = std::env::var("TDSSERVER").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("TDSPORT")
        .unwrap_or_else(|_| "1433".to_string())
        .parse()
        .unwrap();
    let user = std::env::var("TDSUSER").unwrap_or_else(|_| "sa".to_string());
    let password = std::env::var("TDSPASSWORD").unwrap_or_else(|_| "TestPass123!".to_string());

    let mut config = Config::new();
    config.host(&host);
    config.port(port);
    config.authentication(AuthMethod::sql_server(&user, &password));
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
