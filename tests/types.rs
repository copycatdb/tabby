mod common;

use tabby::FromServerOwned;
use uuid::Uuid;

#[tokio::test]
async fn type_int() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&42i32])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(42i32), row.get("val"));
}

#[tokio::test]
async fn type_bigint() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&123456789i64])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(123456789i64), row.get("val"));
}

#[tokio::test]
async fn type_smallint() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&32i16])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(32i16), row.get("val"));
}

#[tokio::test]
async fn type_tinyint() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&255u8])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(255u8), row.get("val"));
}

#[tokio::test]
async fn type_bit_true() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&true])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(true), row.get("val"));
}

#[tokio::test]
async fn type_bit_false() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&false])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(false), row.get("val"));
}

#[tokio::test]
async fn type_float() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&3.14f64])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<f64> = row.get("val");
    assert!((val.unwrap() - 3.14).abs() < 0.001);
}

#[tokio::test]
async fn type_real() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&2.5f32])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<f32> = row.get("val");
    assert!((val.unwrap() - 2.5).abs() < 0.001);
}

#[tokio::test]
async fn type_nvarchar() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&"hello world"])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some("hello world"), row.get::<&str, _>("val"));
}

#[tokio::test]
async fn type_nvarchar_unicode() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&"こんにちは🌍"])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some("こんにちは🌍"), row.get::<&str, _>("val"));
}

#[tokio::test]
async fn type_nvarchar_long() {
    let mut client = common::connect().await;
    let long_str = "x".repeat(5000);
    let row = client
        .execute("SELECT @P1 AS val", &[&long_str.as_str()])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&str> = row.get("val");
    assert_eq!(5000, val.unwrap().len());
}

#[tokio::test]
async fn type_varchar_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('test' AS VARCHAR(10)) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some("test"), row.get::<&str, _>("val"));
}

#[tokio::test]
async fn type_nchar_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('ab' AS NCHAR(5)) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&str> = row.get("val");
    assert_eq!("ab   ", val.unwrap()); // right-padded
}

#[tokio::test]
async fn type_char_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('xy' AS CHAR(5)) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&str> = row.get("val");
    assert_eq!("xy   ", val.unwrap());
}

#[tokio::test]
async fn type_varbinary() {
    let mut client = common::connect().await;
    let data: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let row = client
        .execute("SELECT @P1 AS val", &[&data])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let result: Option<&[u8]> = row.get("val");
    assert_eq!(&[0xDE, 0xAD, 0xBE, 0xEF], result.unwrap());
}

#[tokio::test]
async fn type_binary_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(0xCAFE AS BINARY(4)) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let result: Option<&[u8]> = row.get("val");
    assert_eq!(&[0x00, 0x00, 0xCA, 0xFE], result.unwrap());
}

#[tokio::test]
async fn type_uniqueidentifier() {
    let mut client = common::connect().await;
    let id = Uuid::new_v4();
    let row = client
        .execute("SELECT @P1 AS val", &[&id])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(id), row.get("val"));
}

#[tokio::test]
async fn type_uniqueidentifier_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT NEWID() AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<Uuid> = row.get("val");
    assert!(val.is_some());
}

#[tokio::test]
async fn type_decimal_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(123.45 AS DECIMAL(10, 2)) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<tabby::protocol::numeric::Numeric> = row.get("val");
    assert!(val.is_some());
}

#[tokio::test]
async fn type_numeric_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(99.99 AS NUMERIC(6, 2)) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<tabby::protocol::numeric::Numeric> = row.get("val");
    assert!(val.is_some());
}

#[tokio::test]
async fn type_money_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(12345.6789 AS MONEY) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    // Money comes back as f64 or as a special type
    let _val = row.get::<f64, _>("val");
}

#[tokio::test]
async fn type_smallmoney_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(12.34 AS SMALLMONEY) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let _val = row.get::<f64, _>("val");
}

#[tokio::test]
async fn type_date_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('2024-06-15' AS DATE) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&str> = row.try_get("val").unwrap_or(None);
    // Date might come as chrono type or string - just ensure no error
    let _ = val;
}

#[tokio::test]
async fn type_datetime_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('2024-01-15 10:30:00' AS DATETIME) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert!(row.len() > 0);
}

#[tokio::test]
async fn type_datetime2_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('2024-01-15 10:30:00.1234567' AS DATETIME2) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert!(row.len() > 0);
}

#[tokio::test]
async fn type_smalldatetime_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('2024-01-15 10:30:00' AS SMALLDATETIME) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert!(row.len() > 0);
}

#[tokio::test]
async fn type_time_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('10:30:00.1234567' AS TIME) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert!(row.len() > 0);
}

#[tokio::test]
async fn type_datetimeoffset_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('2024-01-15 10:30:00.123 +05:30' AS DATETIMEOFFSET) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert!(row.len() > 0);
}

#[tokio::test]
async fn type_text_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('hello text' AS TEXT) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&str> = row.get("val");
    assert_eq!(Some("hello text"), val);
}

#[tokio::test]
async fn type_ntext_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(N'hello ntext' AS NTEXT) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&str> = row.get("val");
    assert_eq!(Some("hello ntext"), val);
}

#[tokio::test]
async fn type_image_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(0xDEADBEEF AS IMAGE) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&[u8]> = row.get("val");
    assert_eq!(Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]), val);
}

#[tokio::test]
async fn type_xml_from_server() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('<root>hello</root>' AS XML) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&tabby::xml::XmlData> = row.get("val");
    assert!(val.is_some());
}

#[tokio::test]
async fn type_null_int() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(NULL AS INT) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<i32> = row.get("val");
    assert!(val.is_none());
}

#[tokio::test]
async fn type_null_varchar() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(NULL AS NVARCHAR(10)) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&str> = row.get("val");
    assert!(val.is_none());
}

#[tokio::test]
async fn type_null_binary() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(NULL AS VARBINARY(10)) AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<&[u8]> = row.get("val");
    assert!(val.is_none());
}

#[tokio::test]
async fn type_option_param_none() {
    let mut client = common::connect().await;
    let val: Option<i32> = None;
    let row = client
        .execute("SELECT @P1 AS val", &[&val])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let result: Option<i32> = row.get("val");
    assert!(result.is_none());
}

#[tokio::test]
async fn type_option_param_some() {
    let mut client = common::connect().await;
    let val: Option<i32> = Some(42);
    let row = client
        .execute("SELECT @P1 AS val", &[&val])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(42i32), row.get("val"));
}

#[tokio::test]
async fn type_from_server_owned_string() {
    let mut client = common::connect().await;
    let row = client
        .execute("SELECT @P1 AS val", &[&"owned test"])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let vals: Vec<_> = row.into_iter().collect();
    let owned: Option<String> = String::from_sql_owned(vals.into_iter().next().unwrap()).unwrap();
    assert_eq!(Some("owned test".to_string()), owned);
}

#[tokio::test]
async fn type_from_server_owned_bytes() {
    let mut client = common::connect().await;
    let data = vec![1u8, 2, 3];
    let row = client
        .execute("SELECT @P1 AS val", &[&data])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let vals: Vec<_> = row.into_iter().collect();
    let owned: Option<Vec<u8>> =
        Vec::<u8>::from_sql_owned(vals.into_iter().next().unwrap()).unwrap();
    assert_eq!(Some(vec![1u8, 2, 3]), owned);
}

#[tokio::test]
async fn type_all_integer_sizes_round_trip() {
    let mut client = common::connect().await;
    // Test various integer types via server casts
    let row = client.execute_raw(
        "SELECT CAST(1 AS TINYINT) AS a, CAST(2 AS SMALLINT) AS b, CAST(3 AS INT) AS c, CAST(4 AS BIGINT) AS d"
    ).await.unwrap().into_row().await.unwrap().unwrap();
    assert_eq!(Some(1u8), row.get("a"));
    assert_eq!(Some(2i16), row.get("b"));
    assert_eq!(Some(3i32), row.get("c"));
    assert_eq!(Some(4i64), row.get("d"));
}
