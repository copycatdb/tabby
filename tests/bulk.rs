mod common;

use tabby::IntoRowMessage;

#[tokio::test]
async fn bulk_insert_basic() {
    let mut client = common::connect().await;

    client
        .execute_raw("CREATE TABLE ##bulk_test (id INT, name NVARCHAR(50))")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    let mut req = client.bulk_insert("##bulk_test").await.unwrap();

    for i in 0..10i32 {
        let row = (i, format!("name_{}", i)).into_row();
        req.send(row).await.unwrap();
    }

    let result = req.finalize().await.unwrap();
    assert_eq!(10, result.total());

    // Verify data
    let stream = client
        .execute_raw("SELECT COUNT(*) AS cnt FROM ##bulk_test")
        .await
        .unwrap();
    let row = stream.into_row().await.unwrap().unwrap();
    assert_eq!(Some(10i32), row.get("cnt"));
}

#[tokio::test]
async fn bulk_insert_empty() {
    let mut client = common::connect().await;

    client
        .execute_raw("CREATE TABLE ##bulk_empty (id INT)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    let req = client.bulk_insert("##bulk_empty").await.unwrap();
    let result = req.finalize().await.unwrap();
    assert_eq!(0, result.total());
}

#[tokio::test]
async fn bulk_insert_various_types() {
    let mut client = common::connect().await;

    client
        .execute_raw("CREATE TABLE ##bulk_types (a INT, b BIGINT, c NVARCHAR(100), d BIT, e FLOAT)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    let mut req = client.bulk_insert("##bulk_types").await.unwrap();
    let row = (42i32, 999i64, "hello".to_string(), true, 3.14f64).into_row();
    req.send(row).await.unwrap();
    let result = req.finalize().await.unwrap();
    assert_eq!(1, result.total());

    let row = client
        .execute_raw("SELECT * FROM ##bulk_types")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(42i32), row.get("a"));
    assert_eq!(Some(999i64), row.get("b"));
    assert_eq!(Some("hello"), row.get::<&str, _>("c"));
    assert_eq!(Some(true), row.get("d"));
}
