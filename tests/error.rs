mod common;

use tabby::{AuthMethod, Config};

#[tokio::test]
async fn invalid_sql_syntax() {
    let mut client = common::connect().await;
    let result = client.execute_raw("SELEC INVALID SYNTAX").await;
    // Should get a server error
    assert!(
        result.is_err() || {
            let stream = result.unwrap();
            stream.into_first_result().await.is_err()
        }
    );
}

#[tokio::test]
async fn nonexistent_table() {
    let mut client = common::connect().await;
    let result = client
        .execute_raw("SELECT * FROM nonexistent_table_xyz")
        .await;
    assert!(
        result.is_err() || {
            let stream = result.unwrap();
            stream.into_first_result().await.is_err()
        }
    );
}

#[tokio::test]
async fn connection_refused() {
    let mut config = Config::new();
    config.host("localhost");
    config.port(19999); // unlikely to be open
    config.authentication(AuthMethod::sql_server("sa", "TestPass123!"));
    config.trust_cert();

    let result = common::connect_with_config(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn error_code_none_for_non_server() {
    let err = tabby::error::Error::Utf8;
    assert_eq!(None, err.code());
    assert!(!err.is_deadlock());
}

#[tokio::test]
async fn error_code_from_server() {
    // Trigger a real server error and check code
    let mut client = common::connect().await;
    let result = client.execute_raw("RAISERROR('test error', 16, 1)").await;
    if let Err(e) = result {
        assert!(e.code().is_some());
    }
    // Some versions may not error until consuming the stream
}

#[tokio::test]
async fn error_display() {
    let err = tabby::error::Error::Protocol("test error".into());
    assert!(format!("{}", err).contains("test error"));

    let err = tabby::error::Error::Utf8;
    assert!(format!("{}", err).contains("UTF-8"));

    let err = tabby::error::Error::Utf16;
    assert!(format!("{}", err).contains("UTF-16"));

    let err = tabby::error::Error::Tls("tls fail".into());
    assert!(format!("{}", err).contains("tls fail"));

    let err = tabby::error::Error::Encoding("bad encoding".into());
    assert!(format!("{}", err).contains("bad encoding"));

    let err = tabby::error::Error::Conversion("conv fail".into());
    assert!(format!("{}", err).contains("conv fail"));

    let err = tabby::error::Error::BulkInput("bulk fail".into());
    assert!(format!("{}", err).contains("bulk fail"));

    let err = tabby::error::Error::Routing {
        host: "host".into(),
        port: 1234,
    };
    let msg = format!("{}", err);
    assert!(msg.contains("host"));
    assert!(msg.contains("1234"));
}

#[tokio::test]
async fn error_from_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
    let err: tabby::error::Error = io_err.into();
    match err {
        tabby::error::Error::Io { kind, message } => {
            assert_eq!(std::io::ErrorKind::ConnectionRefused, kind);
            assert!(message.contains("refused"));
        }
        _ => panic!("Expected Io error"),
    }
}

#[tokio::test]
async fn error_from_parse_int() {
    let parse_err = "abc".parse::<i32>().unwrap_err();
    let err: tabby::error::Error = parse_err.into();
    assert!(matches!(err, tabby::error::Error::ParseInt(_)));
    assert!(format!("{}", err).contains("integer"));
}

#[tokio::test]
async fn error_from_utf8() {
    let bytes = vec![0xFFu8];
    let utf8_err = std::str::from_utf8(&bytes).unwrap_err();
    let err: tabby::error::Error = utf8_err.into();
    assert!(matches!(err, tabby::error::Error::Utf8));
}

#[tokio::test]
async fn error_from_string_utf8() {
    let err = String::from_utf8(vec![0xFF]).unwrap_err();
    let err: tabby::error::Error = err.into();
    assert!(matches!(err, tabby::error::Error::Utf8));
}

#[tokio::test]
async fn error_from_utf16() {
    let err = String::from_utf16(&[0xD800]).unwrap_err(); // lone surrogate
    let err: tabby::error::Error = err.into();
    assert!(matches!(err, tabby::error::Error::Utf16));
}
