mod common;

#[tokio::test]
async fn simple_select() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT 42 AS answer")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(42i32), row.get("answer"));
}

#[tokio::test]
async fn parameterized_query() {
    let mut client = common::connect().await;
    let stream = client
        .execute("SELECT @P1 AS a, @P2 AS b", &[&1i32, &"hello"])
        .await
        .unwrap();
    let row = stream.into_row().await.unwrap().unwrap();
    assert_eq!(Some(1i32), row.get("a"));
    assert_eq!(Some("hello"), row.get::<&str, _>("b"));
}

#[tokio::test]
async fn multiple_rows() {
    let mut client = common::connect().await;
    let stream = client
        .execute_raw("SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await
        .unwrap();
    let rows = stream.into_first_result().await.unwrap();
    assert_eq!(3, rows.len());
    assert_eq!(Some(1i32), rows[0].get("v"));
    assert_eq!(Some(2i32), rows[1].get("v"));
    assert_eq!(Some(3i32), rows[2].get("v"));
}

#[tokio::test]
async fn empty_result() {
    let mut client = common::connect().await;
    let stream = client
        .execute_raw("SELECT 1 AS v WHERE 1 = 0")
        .await
        .unwrap();
    let rows = stream.into_first_result().await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn multiple_result_sets() {
    let mut client = common::connect().await;
    let stream = client
        .execute_raw("SELECT 1 AS a; SELECT 'two' AS b")
        .await
        .unwrap();
    let results = stream.into_results().await.unwrap();
    assert_eq!(2, results.len());
    assert_eq!(Some(1i32), results[0][0].get("a"));
    assert_eq!(Some("two"), results[1][0].get::<&str, _>("b"));
}

#[tokio::test]
async fn execute_insert_update_delete() {
    let mut client = common::connect().await;
    client
        .execute_raw("CREATE TABLE ##test_iud (id INT, name NVARCHAR(50))")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    let result = client
        .run(
            "INSERT INTO ##test_iud VALUES (@P1, @P2)",
            &[&1i32, &"alice"],
        )
        .await
        .unwrap();
    assert_eq!(1, result.total());

    let result = client
        .run(
            "UPDATE ##test_iud SET name = @P1 WHERE id = @P2",
            &[&"bob", &1i32],
        )
        .await
        .unwrap();
    assert_eq!(1, result.total());

    let result = client
        .run("DELETE FROM ##test_iud WHERE id = @P1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(1, result.total());
}

#[tokio::test]
async fn row_columns_and_len() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT 1 AS foo, 2 AS bar, 3 AS baz")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(3, row.len());
    assert_eq!("foo", row.columns()[0].name());
    assert_eq!("bar", row.columns()[1].name());
    assert_eq!("baz", row.columns()[2].name());
}

#[tokio::test]
async fn row_get_by_index() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT 10 AS a, 20 AS b")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(10i32), row.get(0));
    assert_eq!(Some(20i32), row.get(1));
}

#[tokio::test]
async fn row_try_get_invalid_column() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT 1 AS a")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let result = row.try_get::<i32, _>("nonexistent");
    assert!(result.is_err());
}

#[tokio::test]
async fn null_values() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT NULL AS val")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let val: Option<i32> = row.get(0);
    assert!(val.is_none());
}

#[tokio::test]
async fn row_into_iterator() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT 1 AS a, 2 AS b")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let vals: Vec<_> = row.into_iter().collect();
    assert_eq!(2, vals.len());
}

#[tokio::test]
async fn row_cells() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT 1 AS a, 2 AS b")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let cells: Vec<_> = row.cells().collect();
    assert_eq!(2, cells.len());
    assert_eq!("a", cells[0].0.name());
}

#[tokio::test]
async fn result_index() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT 1 AS a")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(0, row.result_index());
}

#[tokio::test]
async fn execute_result_rows_affected() {
    let mut client = common::connect().await;
    client
        .execute_raw("CREATE TABLE ##test_ra (id INT)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let result = client
        .run(
            "INSERT INTO ##test_ra VALUES (1); INSERT INTO ##test_ra VALUES (2)",
            &[],
        )
        .await
        .unwrap();
    let affected = result.rows_affected();
    assert!(affected.len() >= 2);
}

#[tokio::test]
async fn execute_result_into_iter() {
    let mut client = common::connect().await;
    client
        .execute_raw("CREATE TABLE ##test_ri (id INT)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let result = client
        .run("INSERT INTO ##test_ri VALUES (1)", &[])
        .await
        .unwrap();
    let counts: Vec<u64> = result.into_iter().collect();
    assert!(!counts.is_empty());
}

#[tokio::test]
async fn query_object() {
    let mut client = common::connect().await;
    let mut query = tabby::Query::new("SELECT @P1 AS val");
    query.bind(42i32);
    let row = query
        .query(&mut client)
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(42i32), row.get("val"));
}

#[tokio::test]
async fn query_object_execute() {
    let mut client = common::connect().await;
    client
        .execute_raw("CREATE TABLE ##test_qoe (id INT)")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let mut query = tabby::Query::new("INSERT INTO ##test_qoe VALUES (@P1)");
    query.bind(1i32);
    let result = query.execute(&mut client).await.unwrap();
    assert_eq!(1, result.total());
}
