mod common;

use tabby::{AuthMethod, Config};

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
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();
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
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();
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
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "WrongPassword!"));
    config.trust_cert();

    let result = common::connect_with_config(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn auth_failure_wrong_user() {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("nonexistent_user", "TestPass123!"));
    config.trust_cert();

    let result = common::connect_with_config(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn config_builder_defaults() {
    let config = Config::new();
    assert_eq!("localhost:1433", config.get_addr());
}

#[tokio::test]
async fn config_builder_custom() {
    let mut config = Config::new();
    config.host("myserver");
    config.port(5555);
    assert_eq!("myserver:5555", config.get_addr());
}

#[tokio::test]
async fn config_instance_name_uses_browser_port() {
    let mut config = Config::new();
    config.instance_name("MYINSTANCE");
    // When instance_name is set and no port, defaults to 1434
    assert_eq!("localhost:1434", config.get_addr());
}

#[tokio::test]
async fn config_port_overrides_instance_name() {
    let mut config = Config::new();
    config.instance_name("MYINSTANCE");
    config.port(9999);
    assert_eq!("localhost:9999", config.get_addr());
}

#[tokio::test]
async fn config_dot_host_becomes_localhost() {
    let mut config = Config::new();
    config.host(".");
    assert_eq!("localhost:1433", config.get_addr());
}

#[tokio::test]
async fn readonly_config() {
    let mut config = Config::new();
    config.readonly(true);
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();

    // Should connect fine
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
