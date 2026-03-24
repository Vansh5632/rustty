use clap::Parser;
use rust_db_core::{Operator, Value};
use rust_db_storage::LsmStorage;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::collections::HashMap;
use std::path::PathBuf;

/// RustDB — An LSM-tree based database engine with MVCC, WASM stored procedures, and RBAC.
#[derive(Parser)]
#[command(name = "rustdb", version, about)]
struct Cli {
    /// Path to the data directory
    #[arg(short, long, env = "RUSTDB_DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,

    /// Run the built-in demo and exit
    #[arg(long)]
    demo: bool,
}

// Generic row: table → {column → value} stored as JSON-ish map
type Row = HashMap<String, Value>;

struct DbState {
    storage: LsmStorage,
    /// Schema registry: table_name → list of column names
    tables: HashMap<String, Vec<String>>,
    /// Auto-increment counters per table
    counters: HashMap<String, u64>,
}

impl DbState {
    fn new(storage: LsmStorage) -> Self {
        Self {
            storage,
            tables: HashMap::new(),
            counters: HashMap::new(),
        }
    }

    fn next_id(&mut self, table: &str) -> u64 {
        let counter = self.counters.entry(table.to_string()).or_insert(0);
        *counter += 1;
        *counter
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let storage = LsmStorage::new(&cli.data_dir)?;
    let mut state = DbState::new(storage);

    if cli.demo {
        return run_demo(&mut state).await;
    }

    println!("RustDB v{}", env!("CARGO_PKG_VERSION"));
    println!("Data directory: {}", cli.data_dir.display());
    println!("Type 'help' for available commands.\n");

    let mut rl = DefaultEditor::new()?;
    let history_path = cli.data_dir.join(".rustdb_history");
    let _ = rl.load_history(&history_path);

    loop {
        match rl.readline("rustdb> ") {
            Ok(line) => {
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(&line);

                match execute_command(&mut state, &line).await {
                    Ok(output) => {
                        if !output.is_empty() {
                            println!("{output}");
                        }
                    }
                    Err(e) => eprintln!("Error: {e}"),
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                println!("Bye!");
                break;
            }
            Err(e) => {
                eprintln!("Readline error: {e}");
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
    Ok(())
}

// ---------------------------------------------------------------------------
// Command dispatcher
// ---------------------------------------------------------------------------
async fn execute_command(state: &mut DbState, input: &str) -> anyhow::Result<String> {
    // Split into tokens respecting single-quoted strings
    let tokens = tokenize(input);
    if tokens.is_empty() {
        return Ok(String::new());
    }

    let cmd = tokens[0].to_uppercase();
    match cmd.as_str() {
        "HELP" => Ok(help_text()),
        "CREATE" => cmd_create_table(state, &tokens).await,
        "INSERT" => cmd_insert(state, &tokens).await,
        "SELECT" => cmd_select(state, &tokens).await,
        "GET" => cmd_get(state, &tokens).await,
        "DELETE" => cmd_delete(state, &tokens).await,
        "TABLES" => cmd_tables(state),
        "COUNT" => cmd_count(state, &tokens).await,
        "DROP" => cmd_drop_table(state, &tokens).await,
        "DEMO" => {
            Box::pin(run_demo(state)).await?;
            Ok(String::new())
        }
        "EXIT" | "QUIT" | "\\Q" => std::process::exit(0),
        _ => Err(anyhow::anyhow!(
            "Unknown command '{cmd}'. Type 'help' for available commands."
        )),
    }
}

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------
fn help_text() -> String {
    r#"
  RustDB REPL Commands
  ====================

  CREATE TABLE <name> (col1, col2, ...)  Create a new table with columns
  INSERT INTO <table> (col1, col2, ...) VALUES (v1, v2, ...)
                                          Insert a row
  SELECT FROM <table> [WHERE col op val [AND col op val ...]] [LIMIT n]
                                          Query rows
  GET <table> <id>                        Get a single row by auto-id
  DELETE FROM <table> <id>                Delete a row by id
  COUNT <table>                           Count rows in a table
  DROP TABLE <table>                      Remove a table (and its data)
  TABLES                                  List all tables
  DEMO                                    Run built-in demo
  HELP                                    Show this message
  EXIT / QUIT                             Exit the REPL

  Operators: =  !=  >  <  >=  <=  CONTAINS  STARTSWITH  ENDSWITH

  Values: integers (42), floats (3.14), strings ('hello'), booleans (true/false)

  Example session:
    rustdb> CREATE TABLE users (name, age, email)
    rustdb> INSERT INTO users (name, age, email) VALUES ('Alice', 30, 'alice@example.com')
    rustdb> SELECT FROM users WHERE age > 25
    rustdb> GET users 1
    rustdb> DELETE FROM users 1
"#
    .to_string()
}

// ---------------------------------------------------------------------------
// CREATE TABLE <name> (col1, col2, ...)
// ---------------------------------------------------------------------------
async fn cmd_create_table(state: &mut DbState, tokens: &[String]) -> anyhow::Result<String> {
    // CREATE TABLE <name> (col1, col2, ...)
    if tokens.len() < 3 || tokens[1].to_uppercase() != "TABLE" {
        return Err(anyhow::anyhow!(
            "Syntax: CREATE TABLE <name> (col1, col2, ...)"
        ));
    }
    let table_name = tokens[2].to_lowercase();
    if state.tables.contains_key(&table_name) {
        return Err(anyhow::anyhow!("Table '{table_name}' already exists"));
    }

    // Parse columns from remaining tokens, stripping parens and commas
    let cols_raw: String = tokens[3..].join(" ");
    let cols = parse_column_list(&cols_raw)?;
    if cols.is_empty() {
        return Err(anyhow::anyhow!("Must specify at least one column"));
    }

    // Store schema
    let schema_key = format!("__schema__:{table_name}").into_bytes();
    let schema_val = serde_json::to_vec(&cols).unwrap();
    state.storage.put(&schema_key, &schema_val).await?;

    state.tables.insert(table_name.clone(), cols.clone());
    state.counters.insert(table_name.clone(), 0);

    Ok(format!(
        "Created table '{table_name}' with columns: {}",
        cols.join(", ")
    ))
}

// ---------------------------------------------------------------------------
// INSERT INTO <table> (col1, col2, ...) VALUES (v1, v2, ...)
// ---------------------------------------------------------------------------
async fn cmd_insert(state: &mut DbState, tokens: &[String]) -> anyhow::Result<String> {
    // INSERT INTO <table> (cols...) VALUES (vals...)
    if tokens.len() < 4 || tokens[1].to_uppercase() != "INTO" {
        return Err(anyhow::anyhow!(
            "Syntax: INSERT INTO <table> (col1, ..) VALUES (v1, ..)"
        ));
    }
    let table_name = tokens[2].to_lowercase();
    let schema = state
        .tables
        .get(&table_name)
        .ok_or_else(|| anyhow::anyhow!("Table '{table_name}' does not exist"))?
        .clone();

    // Everything after the table name
    let rest: String = tokens[3..].join(" ");

    // Split at VALUES
    let upper = rest.to_uppercase();
    let values_pos = upper
        .find("VALUES")
        .ok_or_else(|| anyhow::anyhow!("Missing VALUES keyword"))?;
    let cols_part = &rest[..values_pos];
    let vals_part = &rest[values_pos + 6..]; // skip "VALUES"

    let col_names = parse_column_list(cols_part)?;
    let val_strings = parse_value_list(vals_part)?;

    if col_names.len() != val_strings.len() {
        return Err(anyhow::anyhow!(
            "Column count ({}) doesn't match value count ({})",
            col_names.len(),
            val_strings.len()
        ));
    }

    // Verify columns exist in schema
    for c in &col_names {
        if !schema.contains(c) {
            return Err(anyhow::anyhow!(
                "Column '{c}' not in table '{table_name}' (columns: {})",
                schema.join(", ")
            ));
        }
    }

    let id = state.next_id(&table_name);
    let mut row = Row::new();
    row.insert("_id".to_string(), Value::Int(id as i64));
    for (c, v) in col_names.iter().zip(val_strings.iter()) {
        row.insert(c.clone(), parse_value(v));
    }

    let key = format!("{table_name}:{id}").into_bytes();
    let encoded = bincode::serialize(&row)?;
    state.storage.put(&key, &encoded).await?;

    // Update counter key for persistence
    let counter_key = format!("__counter__:{table_name}").into_bytes();
    let counter_val = bincode::serialize(&id)?;
    state.storage.put(&counter_key, &counter_val).await?;

    Ok(format!("Inserted row with _id={id} into '{table_name}'"))
}

// ---------------------------------------------------------------------------
// SELECT FROM <table> [WHERE col op val ...] [LIMIT n]
// ---------------------------------------------------------------------------
async fn cmd_select(state: &mut DbState, tokens: &[String]) -> anyhow::Result<String> {
    if tokens.len() < 3 || tokens[1].to_uppercase() != "FROM" {
        return Err(anyhow::anyhow!(
            "Syntax: SELECT FROM <table> [WHERE col op val [AND ...]] [LIMIT n]"
        ));
    }
    let table_name = tokens[2].to_lowercase();
    if !state.tables.contains_key(&table_name) {
        return Err(anyhow::anyhow!("Table '{table_name}' does not exist"));
    }

    // Parse WHERE and LIMIT
    let (filters, limit) = parse_where_clause(&tokens[3..])?;

    // Scan all rows
    let prefix = format!("{table_name}:").into_bytes();
    let rows = state.storage.scan(&prefix).await?;

    let schema = state.tables.get(&table_name).unwrap();
    let mut matched: Vec<Row> = Vec::new();

    for (_key, value) in &rows {
        if value.is_empty() {
            continue;
        }
        let row: Row = match bincode::deserialize(value) {
            Ok(r) => r,
            Err(_) => continue,
        };
        if apply_filters(&row, &filters) {
            matched.push(row);
            if let Some(lim) = limit {
                if matched.len() >= lim {
                    break;
                }
            }
        }
    }

    if matched.is_empty() {
        return Ok("(0 rows)".to_string());
    }

    Ok(format_table(schema, &matched))
}

// ---------------------------------------------------------------------------
// GET <table> <id>
// ---------------------------------------------------------------------------
async fn cmd_get(state: &mut DbState, tokens: &[String]) -> anyhow::Result<String> {
    if tokens.len() < 3 {
        return Err(anyhow::anyhow!("Syntax: GET <table> <id>"));
    }
    let table_name = tokens[1].to_lowercase();
    let schema = state
        .tables
        .get(&table_name)
        .ok_or_else(|| anyhow::anyhow!("Table '{table_name}' does not exist"))?;

    let id = &tokens[2];
    let key = format!("{table_name}:{id}").into_bytes();
    let data = state.storage.scan(&key).await?;

    for (_k, v) in &data {
        if v.is_empty() {
            continue;
        }
        let row: Row = bincode::deserialize(v)?;
        return Ok(format_single_row(schema, &row));
    }
    Ok(format!("No row with _id={id} in '{table_name}'"))
}

// ---------------------------------------------------------------------------
// DELETE FROM <table> <id>
// ---------------------------------------------------------------------------
async fn cmd_delete(state: &mut DbState, tokens: &[String]) -> anyhow::Result<String> {
    if tokens.len() < 4 || tokens[1].to_uppercase() != "FROM" {
        return Err(anyhow::anyhow!("Syntax: DELETE FROM <table> <id>"));
    }
    let table_name = tokens[2].to_lowercase();
    if !state.tables.contains_key(&table_name) {
        return Err(anyhow::anyhow!("Table '{table_name}' does not exist"));
    }

    let id = &tokens[3];
    let key = format!("{table_name}:{id}").into_bytes();
    // Tombstone: write empty value
    state.storage.put(&key, &[]).await?;
    Ok(format!("Deleted row _id={id} from '{table_name}'"))
}

// ---------------------------------------------------------------------------
// COUNT <table>
// ---------------------------------------------------------------------------
async fn cmd_count(state: &mut DbState, tokens: &[String]) -> anyhow::Result<String> {
    if tokens.len() < 2 {
        return Err(anyhow::anyhow!("Syntax: COUNT <table>"));
    }
    let table_name = tokens[1].to_lowercase();
    if !state.tables.contains_key(&table_name) {
        return Err(anyhow::anyhow!("Table '{table_name}' does not exist"));
    }
    let prefix = format!("{table_name}:").into_bytes();
    let rows = state.storage.scan(&prefix).await?;
    let count = rows.iter().filter(|(_, v)| !v.is_empty()).count();
    Ok(format!("{count}"))
}

// ---------------------------------------------------------------------------
// DROP TABLE <table>
// ---------------------------------------------------------------------------
async fn cmd_drop_table(state: &mut DbState, tokens: &[String]) -> anyhow::Result<String> {
    if tokens.len() < 3 || tokens[1].to_uppercase() != "TABLE" {
        return Err(anyhow::anyhow!("Syntax: DROP TABLE <table>"));
    }
    let table_name = tokens[2].to_lowercase();
    if !state.tables.contains_key(&table_name) {
        return Err(anyhow::anyhow!("Table '{table_name}' does not exist"));
    }

    // Tombstone all rows
    let prefix = format!("{table_name}:").into_bytes();
    let rows = state.storage.scan(&prefix).await?;
    for (key, _) in rows {
        state.storage.put(&key, &[]).await?;
    }

    // Tombstone schema + counter
    let schema_key = format!("__schema__:{table_name}").into_bytes();
    state.storage.put(&schema_key, &[]).await?;
    let counter_key = format!("__counter__:{table_name}").into_bytes();
    state.storage.put(&counter_key, &[]).await?;

    state.tables.remove(&table_name);
    state.counters.remove(&table_name);

    Ok(format!("Dropped table '{table_name}'"))
}

// ---------------------------------------------------------------------------
// TABLES
// ---------------------------------------------------------------------------
fn cmd_tables(state: &DbState) -> anyhow::Result<String> {
    if state.tables.is_empty() {
        return Ok("No tables.".to_string());
    }
    let mut out = String::new();
    for (name, cols) in &state.tables {
        out.push_str(&format!("  {name} ({})\n", cols.join(", ")));
    }
    Ok(out.trim_end().to_string())
}

// ---------------------------------------------------------------------------
// Demo
// ---------------------------------------------------------------------------
fn run_demo<'a>(state: &'a mut DbState) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>> {
    Box::pin(async move {
    println!("=== RustDB Demo ===\n");

    let commands = vec![
        "CREATE TABLE users (name, age, email)",
        "INSERT INTO users (name, age, email) VALUES ('Alice', 30, 'alice@example.com')",
        "INSERT INTO users (name, age, email) VALUES ('Bob', 25, 'bob@example.com')",
        "INSERT INTO users (name, age, email) VALUES ('Charlie', 35, 'charlie@example.com')",
        "INSERT INTO users (name, age, email) VALUES ('Diana', 28, 'diana@example.com')",
        "SELECT FROM users",
        "SELECT FROM users WHERE age > 28",
        "SELECT FROM users WHERE name CONTAINS 'li'",
        "SELECT FROM users LIMIT 2",
        "GET users 1",
        "COUNT users",
        "CREATE TABLE products (name, price, category)",
        "INSERT INTO products (name, price, category) VALUES ('Laptop', 999.99, 'electronics')",
        "INSERT INTO products (name, price, category) VALUES ('Book', 29.99, 'education')",
        "INSERT INTO products (name, price, category) VALUES ('Phone', 699.0, 'electronics')",
        "SELECT FROM products WHERE category = 'electronics'",
        "SELECT FROM products WHERE price > 100",
        "TABLES",
        "DELETE FROM users 2",
        "SELECT FROM users",
    ];

    for cmd in commands {
        println!("rustdb> {cmd}");
        match execute_command(state, cmd).await {
            Ok(output) => {
                if !output.is_empty() {
                    println!("{output}");
                }
            }
            Err(e) => eprintln!("Error: {e}"),
        }
        println!();
    }

    println!("=== Demo complete ===");
    Ok(())
    })
}

// ---------------------------------------------------------------------------
// Tokenizer: splits on whitespace but keeps single-quoted strings intact
// ---------------------------------------------------------------------------
fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for ch in input.chars() {
        if ch == '\'' {
            in_quote = !in_quote;
            current.push(ch);
        } else if ch.is_whitespace() && !in_quote {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

// ---------------------------------------------------------------------------
// Parse helpers
// ---------------------------------------------------------------------------
fn parse_column_list(s: &str) -> anyhow::Result<Vec<String>> {
    let s = s.trim();
    let s = s.strip_prefix('(').unwrap_or(s);
    let s = s.strip_suffix(')').unwrap_or(s);
    let cols: Vec<String> = s
        .split(',')
        .map(|c| c.trim().to_lowercase())
        .filter(|c| !c.is_empty())
        .collect();
    Ok(cols)
}

fn parse_value_list(s: &str) -> anyhow::Result<Vec<String>> {
    let s = s.trim();
    let s = s.strip_prefix('(').unwrap_or(s);
    let s = s.strip_suffix(')').unwrap_or(s);

    // Split on commas but respect single quotes
    let mut vals = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in s.chars() {
        if ch == '\'' {
            in_quote = !in_quote;
            current.push(ch);
        } else if ch == ',' && !in_quote {
            vals.push(current.trim().to_string());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    if !current.trim().is_empty() {
        vals.push(current.trim().to_string());
    }
    Ok(vals)
}

fn parse_value(s: &str) -> Value {
    let s = s.trim();
    // String literal
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        return Value::String(s[1..s.len() - 1].to_string());
    }
    // Boolean
    if s.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }
    // Integer
    if let Ok(i) = s.parse::<i64>() {
        return Value::Int(i);
    }
    // Float
    if let Ok(f) = s.parse::<f64>() {
        return Value::Float(f);
    }
    // Fallback to string
    Value::String(s.to_string())
}

fn parse_operator(s: &str) -> anyhow::Result<Operator> {
    match s.to_uppercase().as_str() {
        "=" | "==" | "EQ" => Ok(Operator::Eq),
        "!=" | "<>" | "NE" => Ok(Operator::Ne),
        ">" | "GT" => Ok(Operator::Gt),
        "<" | "LT" => Ok(Operator::Lt),
        ">=" | "GTE" => Ok(Operator::Gte),
        "<=" | "LTE" => Ok(Operator::Lte),
        "CONTAINS" => Ok(Operator::Contains),
        "STARTSWITH" => Ok(Operator::StartsWith),
        "ENDSWITH" => Ok(Operator::EndsWith),
        _ => Err(anyhow::anyhow!("Unknown operator '{s}'")),
    }
}

struct SimpleFilter {
    field: String,
    operator: Operator,
    value: Value,
}

fn parse_where_clause(tokens: &[String]) -> anyhow::Result<(Vec<SimpleFilter>, Option<usize>)> {
    let mut filters = Vec::new();
    let mut limit: Option<usize> = None;
    let mut i = 0;

    while i < tokens.len() {
        let tok = tokens[i].to_uppercase();
        if tok == "WHERE" || tok == "AND" {
            // Expect: col op val
            if i + 3 >= tokens.len() + 0 {
                // We need at least 3 more tokens
                if i + 3 > tokens.len() {
                    return Err(anyhow::anyhow!(
                        "Incomplete WHERE clause, expected: col operator value"
                    ));
                }
            }
            let field = tokens[i + 1].to_lowercase();
            let op = parse_operator(&tokens[i + 2])?;
            let val = parse_value(&tokens[i + 3]);
            filters.push(SimpleFilter {
                field,
                operator: op,
                value: val,
            });
            i += 4;
        } else if tok == "LIMIT" {
            if i + 1 >= tokens.len() {
                return Err(anyhow::anyhow!("LIMIT requires a number"));
            }
            limit = Some(
                tokens[i + 1]
                    .parse::<usize>()
                    .map_err(|_| anyhow::anyhow!("LIMIT must be a number"))?,
            );
            i += 2;
        } else {
            i += 1;
        }
    }

    Ok((filters, limit))
}

// ---------------------------------------------------------------------------
// Filter application
// ---------------------------------------------------------------------------
fn apply_filters(row: &Row, filters: &[SimpleFilter]) -> bool {
    for f in filters {
        let field_value = match row.get(&f.field) {
            Some(v) => v,
            None => return false,
        };
        let ok = match &f.operator {
            Operator::Eq => field_value == &f.value,
            Operator::Ne => field_value != &f.value,
            Operator::Gt => cmp_gt(field_value, &f.value),
            Operator::Lt => cmp_lt(field_value, &f.value),
            Operator::Gte => cmp_gt(field_value, &f.value) || field_value == &f.value,
            Operator::Lte => cmp_lt(field_value, &f.value) || field_value == &f.value,
            Operator::Contains => match (field_value, &f.value) {
                (Value::String(a), Value::String(b)) => a.contains(b.as_str()),
                _ => false,
            },
            Operator::StartsWith => match (field_value, &f.value) {
                (Value::String(a), Value::String(b)) => a.starts_with(b.as_str()),
                _ => false,
            },
            Operator::EndsWith => match (field_value, &f.value) {
                (Value::String(a), Value::String(b)) => a.ends_with(b.as_str()),
                _ => false,
            },
        };
        if !ok {
            return false;
        }
    }
    true
}

fn cmp_gt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x > y,
        (Value::Float(x), Value::Float(y)) => x > y,
        (Value::Int(x), Value::Float(y)) => (*x as f64) > *y,
        (Value::Float(x), Value::Int(y)) => *x > (*y as f64),
        _ => false,
    }
}

fn cmp_lt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::Int(x), Value::Float(y)) => (*x as f64) < *y,
        (Value::Float(x), Value::Int(y)) => *x < (*y as f64),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Pretty-print helpers
// ---------------------------------------------------------------------------
fn format_table(schema: &[String], rows: &[Row]) -> String {
    // Collect all columns: _id + schema columns
    let mut cols = vec!["_id".to_string()];
    cols.extend(schema.iter().cloned());

    // Compute column widths
    let mut widths: Vec<usize> = cols.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, col) in cols.iter().enumerate() {
            let w = row
                .get(col)
                .map(|v| value_display(v).len())
                .unwrap_or(4); // "NULL"
            if w > widths[i] {
                widths[i] = w;
            }
        }
    }

    let mut out = String::new();

    // Header
    let header: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
        .collect();
    out.push_str(&format!("  {}\n", header.join(" | ")));

    // Separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    out.push_str(&format!("  {}\n", sep.join("-+-")));

    // Rows
    for row in rows {
        let cells: Vec<String> = cols
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let val = row
                    .get(col)
                    .map(|v| value_display(v))
                    .unwrap_or_else(|| "NULL".to_string());
                format!("{:width$}", val, width = widths[i])
            })
            .collect();
        out.push_str(&format!("  {}\n", cells.join(" | ")));
    }

    out.push_str(&format!("({} rows)", rows.len()));
    out
}

fn format_single_row(schema: &[String], row: &Row) -> String {
    let mut out = String::new();
    // _id first
    if let Some(id) = row.get("_id") {
        out.push_str(&format!("  _id: {}\n", value_display(id)));
    }
    for col in schema {
        let val = row
            .get(col)
            .map(|v| value_display(v))
            .unwrap_or_else(|| "NULL".to_string());
        out.push_str(&format!("  {col}: {val}\n"));
    }
    out.trim_end().to_string()
}

fn value_display(v: &Value) -> String {
    match v {
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
    }
}
