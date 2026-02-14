mod common;

#[tokio::test]
async fn transaction_commit() {
    let mut client = common::connect().await;
    client
        .execute_raw("CREATE TABLE ##tx_test (id INT)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    client
        .execute_raw("BEGIN TRANSACTION")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("INSERT INTO ##tx_test VALUES (1)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("INSERT INTO ##tx_test VALUES (2)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("COMMIT")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    let row = client
        .execute_raw("SELECT COUNT(*) AS cnt FROM ##tx_test")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(2i32), row.get("cnt"));
}

#[tokio::test]
async fn transaction_rollback() {
    let mut client = common::connect().await;
    client
        .execute_raw("CREATE TABLE ##tx_rb (id INT)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    client
        .execute_raw("INSERT INTO ##tx_rb VALUES (1)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("BEGIN TRANSACTION")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("INSERT INTO ##tx_rb VALUES (2)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("ROLLBACK")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    let row = client
        .execute_raw("SELECT COUNT(*) AS cnt FROM ##tx_rb")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(1i32), row.get("cnt"));
}

#[tokio::test]
async fn savepoint() {
    let mut client = common::connect().await;
    client
        .execute_raw("CREATE TABLE ##tx_sp (id INT)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    client
        .execute_raw("BEGIN TRANSACTION")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("INSERT INTO ##tx_sp VALUES (1)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("SAVE TRANSACTION sp1")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("INSERT INTO ##tx_sp VALUES (2)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("ROLLBACK TRANSACTION sp1")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    client
        .execute_raw("COMMIT")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    let row = client
        .execute_raw("SELECT COUNT(*) AS cnt FROM ##tx_sp")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(1i32), row.get("cnt"));
}
