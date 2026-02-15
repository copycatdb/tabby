mod common;

use tabby::row_writer::RowWriter;

#[tokio::test]
async fn sql_variant_product_version() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT SERVERPROPERTY('ProductVersion') AS v")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let v: &str = row.get("v").unwrap();
    assert!(!v.is_empty(), "ProductVersion should not be empty");
    println!("ProductVersion: {}", v);
}

#[tokio::test]
async fn sql_variant_edition() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT SERVERPROPERTY('Edition') AS v")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let v: &str = row.get("v").unwrap();
    assert!(!v.is_empty());
    println!("Edition: {}", v);
}

#[tokio::test]
async fn sql_variant_cast_int() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(42 AS sql_variant) AS v")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let v: i32 = row.get("v").unwrap();
    assert_eq!(v, 42);
}

#[tokio::test]
async fn sql_variant_cast_string() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST('hello' AS sql_variant) AS v")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    let v: &str = row.get("v").unwrap();
    assert_eq!(v, "hello");
}

#[tokio::test]
async fn sql_variant_cast_float() {
    let mut client = common::connect().await;
    let row = client
        .execute_raw("SELECT CAST(3.14 AS sql_variant) AS v")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    // SQL Server casts 3.14 as numeric(3,2) in sql_variant
    // It could come back as Numeric
    println!("3.14 variant: {:?}", row.get::<tabby::Numeric, _>("v"));
}

#[tokio::test]
async fn hierarchyid_does_not_panic() {
    let mut client = common::connect().await;
    // hierarchyid is a CLR UDT; this should not panic
    let result = client
        .execute_raw("SELECT CAST('/1/2/' AS hierarchyid) AS h")
        .await;
    match result {
        Ok(stream) => {
            let row = stream.into_row().await.unwrap().unwrap();
            // Should come back as binary
            let bytes: &[u8] = row.get("h").unwrap();
            assert!(!bytes.is_empty());
            println!("hierarchyid bytes: {:?}", bytes);
        }
        Err(e) => {
            // If hierarchyid not available, that's ok
            println!("hierarchyid not available: {}", e);
        }
    }
}

#[tokio::test]
async fn multi_result_set_on_result_set_end() {
    let mut client = common::connect().await;

    struct TestWriter {
        result_set_ends: usize,
        metadata_calls: usize,
        rows: Vec<Vec<String>>,
        current_row: Vec<String>,
    }

    impl RowWriter for TestWriter {
        fn write_null(&mut self, _col: usize) {
            self.current_row.push("NULL".to_string());
        }
        fn write_bool(&mut self, _col: usize, val: bool) {
            self.current_row.push(val.to_string());
        }
        fn write_u8(&mut self, _col: usize, val: u8) {
            self.current_row.push(val.to_string());
        }
        fn write_i16(&mut self, _col: usize, val: i16) {
            self.current_row.push(val.to_string());
        }
        fn write_i32(&mut self, _col: usize, val: i32) {
            self.current_row.push(val.to_string());
        }
        fn write_i64(&mut self, _col: usize, val: i64) {
            self.current_row.push(val.to_string());
        }
        fn write_f32(&mut self, _col: usize, val: f32) {
            self.current_row.push(val.to_string());
        }
        fn write_f64(&mut self, _col: usize, val: f64) {
            self.current_row.push(val.to_string());
        }
        fn write_str(&mut self, _col: usize, val: &str) {
            self.current_row.push(val.to_string());
        }
        fn write_bytes(&mut self, _col: usize, _val: &[u8]) {
            self.current_row.push("<bytes>".to_string());
        }
        fn write_date(&mut self, _col: usize, _days: i32) {
            self.current_row.push("<date>".to_string());
        }
        fn write_time(&mut self, _col: usize, _nanos: i64) {
            self.current_row.push("<time>".to_string());
        }
        fn write_datetime(&mut self, _col: usize, _micros: i64) {
            self.current_row.push("<datetime>".to_string());
        }
        fn write_datetimeoffset(&mut self, _col: usize, _micros: i64, _offset: i16) {
            self.current_row.push("<dto>".to_string());
        }
        fn write_decimal(&mut self, _col: usize, value: i128, _precision: u8, scale: u8) {
            self.current_row.push(format!("{}e-{}", value, scale));
        }
        fn write_guid(&mut self, _col: usize, _bytes: &[u8; 16]) {
            self.current_row.push("<guid>".to_string());
        }
        fn on_metadata(&mut self, _columns: &[tabby::Column]) {
            self.metadata_calls += 1;
        }
        fn on_row_done(&mut self) {
            self.rows.push(std::mem::take(&mut self.current_row));
        }
        fn on_result_set_end(&mut self) {
            self.result_set_ends += 1;
        }
    }

    let mut writer = TestWriter {
        result_set_ends: 0,
        metadata_calls: 0,
        rows: vec![],
        current_row: vec![],
    };

    client
        .batch_into("SELECT 1 AS a; SELECT 'x' AS b, 'y' AS c", &mut writer)
        .await
        .unwrap();

    assert!(
        writer.metadata_calls >= 2,
        "Should get metadata for 2 result sets, got {}",
        writer.metadata_calls
    );
    assert!(
        writer.result_set_ends >= 1,
        "Should get at least 1 on_result_set_end call, got {}",
        writer.result_set_ends
    );
    assert_eq!(writer.rows.len(), 2, "Should get 2 rows total");
    assert_eq!(writer.rows[0], vec!["1"]);
    assert_eq!(writer.rows[1], vec!["x", "y"]);

    println!(
        "metadata_calls: {}, result_set_ends: {}, rows: {:?}",
        writer.metadata_calls, writer.result_set_ends, writer.rows
    );
}
