use dotenvy::dotenv;
use std::env;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::Row;
use sqlx::Column;
use csv::Writer;
use std::error::Error;
use sqlx::TypeInfo;
use chrono;
use std::io::{self, Write};
use std::io::stdin;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut currect_dir = env::current_exe()
        .expect("Could not get currect ")
        .parent()
        .expect("Failed to get parent directory")
        .to_path_buf();
    currect_dir.push(".env");

    let env_path = Path::new(&currect_dir);
    dotenvy::from_path(env_path).ok();

    let db_url = env::var("DATABASE_URL")?;
    let table_name_list_str = env::var("TABLE_NAME")?;
    let csv_output = env::var("CSV_OUTPUT")?;

    let table_name_list: Vec<&str> = table_name_list_str.split(',').map(|s| s.trim()).collect();

    // Create MySQL connection pool
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await?;

    // Fetch all rows
    for each_table in table_name_list{
        export_table(&pool, &each_table, &csv_output).await?;
    }

    println!("Press Enter to exit...");
    let _ = io::stdout().flush();
    let mut input = String::new();
    let _ = stdin().read_line(&mut input);

    Ok(())
}

async fn export_table(pool: &sqlx::MySqlPool, table_name: &str, csv_output: &str) -> Result<(), Box<dyn Error>> {
    let query = format!("SELECT * FROM {}", table_name);
    let rows = sqlx::query(&query).fetch_all(pool).await?;

    let now = chrono::Local::now();

    let mut file_format = format!("{}_{}_{}.csv", csv_output, table_name, now.format("%Y-%m-%d_%H-%M-%S"));

    if csv_output == "" {
        file_format = format!("{}_{}.csv", table_name, now.format("%Y-%m-%d_%H-%M-%S"));
    }

    // Open CSV writer
    let mut wtr = Writer::from_path(file_format)?;

    if let Some(row) = rows.get(0) {
        // Write CSV headers
        let columns = row.columns();
        let headers: Vec<&str> = columns.iter().map(|c| c.name()).collect();
        wtr.write_record(&headers)?;
    }

    for row in rows {
        let columns = row.columns();
        let mut record = vec![];
    
        for col in columns {
            let type_info = col.type_info().name().to_lowercase();
            let value = match type_info.as_str() {
                "int" | "integer" | "bigint" | "smallint" => {
                    row.try_get::<i64, _>(col.name()).map(|v| v.to_string())
                }
                "float" | "double" | "decimal" => {
                    row.try_get::<f64, _>(col.name()).map(|v| v.to_string())
                }
                "bool" | "boolean" => {
                    row.try_get::<bool, _>(col.name()).map(|v| v.to_string())
                }
                "text" | "varchar" | "char" | "longtext" => {
                    row.try_get::<String, _>(col.name())
                }
                "datetime" | "timestamp" => {
                    row.try_get::<chrono::NaiveDateTime, _>(col.name())
                        .map(|v| v.to_string())
                }
                _ => Ok(String::from("[unsupported]")),
            }
            .unwrap_or_else(|_| String::from(""));
    
            record.push(value);
        }
    
        wtr.write_record(&record)?;
    }
    wtr.flush()?;
    println!("✅ Table: {} was exported successfully.", table_name);


    Ok(())
}