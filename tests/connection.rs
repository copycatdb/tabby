mod common;

use tabby::AuthMethod;

#[tokio::test]
async fn basic_connection() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT 1 AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(1i32), row.get("val"));
    client.close().await.unwrap();
}

#[tokio::test]
async fn connection_to_specific_database() {
    let mut config = common::base_config();
    config.database("master");

    let mut client = common::connect_with_config(config).await.unwrap();
    let row = client
        .execute_raw("SELECT DB_NAME() AS db")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let db: Option<&str> = row.get("db");
    assert_eq!(Some("master"), db);
}

#[tokio::test]
async fn connection_with_application_name() {
    let mut config = common::base_config();
    config.application_name("tabby-test-app");

    let mut client = common::connect_with_config(config).await.unwrap();
    let row = client
        .execute_raw("SELECT APP_NAME() AS app")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let app: Option<&str> = row.get("app");
    assert_eq!(Some("tabby-test-app"), app);
}

#[tokio::test]
async fn auth_failure_wrong_password() {
    let (host, port, _, _) = common::test_config();
    let mut config = tabby::Config::new();
    config.host(&host);
    config.port(port);
    config.authentication(AuthMethod::sql_server("sa", "WrongPassword!"));
    config.trust_cert();

    let result = common::connect_with_config(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn auth_failure_wrong_user() {
    let (host, port, _, _) = common::test_config();
    let mut config = tabby::Config::new();
    config.host(&host);
    config.port(port);
    config.authentication(AuthMethod::sql_server("nonexistent_user", "DoesntMatter!"));
    config.trust_cert();

    let result = common::connect_with_config(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn config_builder_defaults() {
    let config = tabby::Config::new();
    assert_eq!("localhost:1433", config.get_addr());
}

#[tokio::test]
async fn config_builder_custom() {
    let mut config = tabby::Config::new();
    config.host("myserver");
    config.port(5555);
    assert_eq!("myserver:5555", config.get_addr());
}

#[tokio::test]
async fn config_instance_name_uses_browser_port() {
    let mut config = tabby::Config::new();
    config.instance_name("MYINSTANCE");
    assert_eq!("localhost:1434", config.get_addr());
}

#[tokio::test]
async fn config_port_overrides_instance_name() {
    let mut config = tabby::Config::new();
    config.instance_name("MYINSTANCE");
    config.port(9999);
    assert_eq!("localhost:9999", config.get_addr());
}

#[tokio::test]
async fn config_dot_host_becomes_localhost() {
    let mut config = tabby::Config::new();
    config.host(".");
    assert_eq!("localhost:1433", config.get_addr());
}

#[tokio::test]
async fn readonly_config() {
    let mut config = common::base_config();
    config.readonly(true);

    let mut client = common::connect_with_config(config).await.unwrap();
    let row = client
        .execute_raw("SELECT 1 AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(1i32), row.get("val"));
}
