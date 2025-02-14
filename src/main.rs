use anyhow::{Context, Result};
use chrono::Local;
use csv::ReaderBuilder;
use dotenvy::dotenv;
use lazy_static::lazy_static;
use regex::Regex;
use sqlx::{mysql::MySqlPoolOptions, MySql, Pool, Row};
use std::{
    collections::HashSet,
    collections::HashMap,
    env,
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

lazy_static! {
    static ref FILENAME_PATTERN: Regex =
        Regex::new(r"^(\d+)_skipAI_(\d+)_(.+\.csv)$").unwrap();
}

/// Struct representing a record to be inserted into the `address` table.
#[derive(Debug)]
struct AddressRecord {
    street: String,
    unit_type: String,
    unit_num: String,
    mail_city: String,
    state: String, // New field for property_address_state
    zip: String,
    latitude: String,
    longitude: String,
    fullname: String,
    fname: String,
    lname: String,
    mailing_address: String,
    mailing_city: String,
    mailing_state: String,
    mailing_zip: String,
    flag: i64,
    dmid: String,
    via: i64,
    map_image_url: String,
}

/// Struct representing a record to be inserted into the `phonequeue` table.
#[derive(Debug)]
struct PhoneQueueRecord {
    phone1: Option<String>,
    phone2: Option<String>,
    phone3: Option<String>,
}

/// Combined record that holds both the address data and its optional phone data.
/// This ensures the ordering is maintained so that each phone record is matched with
/// the correct address row.
#[derive(Debug)]
struct CombinedRecord {
    address: AddressRecord,
    phone: Option<PhoneQueueRecord>,
}

/// Entry point of the application.
/// Handles configuration loading, setting up database connections,
/// processing CSV files, and managing concurrency via lock files.
#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from `.env` file.
    dotenv().ok();

    // Validate and gather configuration from environment variables.
    let config = Config::from_env().context("Failed to load configuration")?;

    // Ensure upload and processed directories exist.
    fs::create_dir_all(&config.upload_dir)
        .with_context(|| format!("Failed to create upload directory: {}", config.upload_dir))?;
    fs::create_dir_all(&config.processed_dir)
        .with_context(|| format!("Failed to create processed directory: {}", config.processed_dir))?;

    // Acquire a lock to prevent concurrent executions.
    let _lock_guard = LockFileGuard::new(&config.lock_file_path)
        .with_context(|| "Failed to acquire process lock")?;

    // Establish a connection pool to the MySQL database.
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to MySQL database")?;

    // Prefetch all phone numbers from the database.
    let mut global_phone_set = prefetch_all_phone_numbers(&pool).await
        .context("Failed to prefetch phone numbers")?;

    // Retrieve list of CSV files to process.
    let files = get_csv_files(&config.upload_dir).context("Failed to retrieve CSV files")?;

    if files.is_empty() {
        eprintln!(
            "[{}] No files to process.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        return Ok(()); // Nothing to do
    }

    // Process each CSV file individually.
    for file_path in files {
        if let Err(e) = process_file(
            &pool,
            &file_path,
            &config.processed_dir,
            config.batch_size,
            config.max_execution_seconds,
            &mut global_phone_set,
        )
        .await
        {
            eprintln!("Error processing file {:?}: {:?}", file_path, e);
            // Attempt to move the problematic file to the processed directory.
            let file_name = file_path.file_name().unwrap_or_default();
            let new_path = Path::new(&config.processed_dir).join(file_name);
            let _ = fs::rename(&file_path, &new_path);
        }
    }

    Ok(())
}

/// Loads configuration from environment variables.
struct Config {
    database_url: String,
    upload_dir: String,
    processed_dir: String,
    lock_file_path: String,
    batch_size: usize,
    max_execution_seconds: u64,
}

impl Config {
    fn from_env() -> Result<Self> {
        fn parse_env_var<T: std::str::FromStr>(
            key: &str,
            default: Option<T>,
        ) -> Result<T>
        where
            T::Err: std::fmt::Display,
        {
            match env::var(key) {
                Ok(val) => val.parse::<T>().map_err(|e| {
                    anyhow::anyhow!("Invalid value for {}: {}", key, e)
                }),
                Err(_) => match default {
                    Some(d) => Ok(d),
                    None => Err(anyhow::anyhow!("Environment variable {} is required", key)),
                },
            }
        }

        Ok(Self {
            database_url: env::var("DATABASE_URL")
                .context("DATABASE_URL must be set in .env file")?,
            upload_dir: env::var("UPLOAD_DIR").unwrap_or_else(|_| "./uploads".to_string()),
            processed_dir: env::var("PROCESSED_DIR").unwrap_or_else(|_| "./processed".to_string()),
            lock_file_path: env::var("LOCK_FILE").unwrap_or_else(|_| "./process.lock".to_string()),
            batch_size: parse_env_var("BATCH_SIZE", Some(1000))?,
            max_execution_seconds: parse_env_var("MAX_EXECUTION_SECONDS", Some(3600))?,
        })
    }
}

/// Retrieves a list of CSV files from the specified upload directory.
fn get_csv_files(upload_dir: &str) -> Result<Vec<PathBuf>> {
    let pattern = format!("{}/*.csv", upload_dir);
    let files: Vec<PathBuf> = glob::glob(&pattern)?
        .filter_map(Result::ok)
        .collect();
    Ok(files)
}

/// Prefetch all phone numbers (phone1, phone2, phone3) from the phonequeue table.
async fn prefetch_all_phone_numbers(pool: &Pool<MySql>) -> Result<HashSet<String>> {
    let mut set = HashSet::new();
    let rows = sqlx::query("SELECT phone1, phone2, phone3 FROM phonequeue")
        .fetch_all(pool)
        .await
        .context("Failed to prefetch phone numbers")?;
    for row in rows {
        if let Ok(Some(phone)) = row.try_get::<Option<String>, _>("phone1") {
            if !phone.trim().is_empty() {
                set.insert(phone.trim().to_string());
            }
        }
        if let Ok(Some(phone)) = row.try_get::<Option<String>, _>("phone2") {
            if !phone.trim().is_empty() {
                set.insert(phone.trim().to_string());
            }
        }
        if let Ok(Some(phone)) = row.try_get::<Option<String>, _>("phone3") {
            if !phone.trim().is_empty() {
                set.insert(phone.trim().to_string());
            }
        }
    }
    Ok(set)
}

/// Processes a single CSV file: parsing, validating, batching inserts,
/// handling errors, and moving the file post-processing.
async fn process_file(
    pool: &Pool<MySql>,
    file_path: &Path,
    processed_dir: &str,
    batch_size: usize,
    max_execution_seconds: u64,
    global_phone_set: &mut HashSet<String>,
) -> Result<()> {
    let file_name = file_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    // Validate filename against the expected pattern.
    let captures = match FILENAME_PATTERN.captures(&file_name) {
        Some(cap) => cap,
        None => {
            eprintln!("Filename pattern mismatch: {}", file_name);
            let new_path = Path::new(processed_dir).join(&file_name);
            fs::rename(file_path, new_path)?;
            return Ok(());
        }
    };

    let _timestamp = captures.get(1).unwrap().as_str();
    let skip_ai_flag: i64 = captures
        .get(2)
        .unwrap()
        .as_str()
        .parse()
        .unwrap_or(0);
    let original_filename = captures.get(3).unwrap().as_str();

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)
        .with_context(|| format!("Failed to open CSV file: {}", file_name))?;

    let headers = rdr.headers()?.clone();
    let header_map: HashMap<&str, usize> = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| (header.trim(), idx))
        .collect();

    // Define required columns.
    let required_columns = [
        "property_address_line_1",
        "property_address_line_2",
        "property_address_city",
        "property_address_state", // New required column
        "property_address_zipcode",
        "property_lat",
        "property_lng",
        "owner_1_firstname",
        "owner_1_lastname",
        "owner_1_name",
        "owner_address_line_1",
        "owner_address_city",
        "owner_address_state",
        "owner_address_zip",
        "lead_id",
        "owner_2_firstname",
        "owner_2_lastname",
        "owner_2_name",
        "contact_1_phone1",
        "contact_1_phone2",
        "contact_1_phone3",
        "contact_2_phone1",
        "contact_2_phone2",
        "contact_2_phone3",
    ];

    let missing_columns: Vec<&str> = required_columns
        .iter()
        .filter(|col| !header_map.contains_key(*col))
        .copied()
        .collect();
    if !missing_columns.is_empty() {
        eprintln!(
            "Missing required columns in {}: {:?}",
            file_name, missing_columns
        );
        let new_path = Path::new(processed_dir).join(&file_name);
        fs::rename(file_path, new_path)?;
        return Ok(());
    }

    let campaign_name = Path::new(original_filename)
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let start_time = Instant::now();

    let (_campaign_id, new_flag) = ensure_campaign(pool, &campaign_name).await
        .context("Failed to ensure campaign exists")?;

    let mut existing_dmids = prefetch_dmids(pool, new_flag).await
        .context("Failed to prefetch DMIDs")?;

    // Combined batch for address and phone data.
    let mut combined_batch: Vec<CombinedRecord> = Vec::with_capacity(batch_size);
    let mut row_counter = 0_usize;
    let mut processed_rows = 0_usize;

    for result in rdr.records() {
        let record = match result {
            Ok(rec) => rec,
            Err(e) => {
                eprintln!("Skipping malformed line in {}: {:?}", file_name, e);
                continue;
            }
        };
        row_counter += 1;

        if start_time.elapsed() > Duration::from_secs(max_execution_seconds) {
            eprintln!(
                "Script timeout after {} seconds while processing {}.",
                max_execution_seconds, file_name
            );
            break;
        }

        let lead_id = record.get(*header_map.get("lead_id").unwrap()).unwrap_or("").trim();
        if lead_id.is_empty() {
            continue;
        }
        if existing_dmids.contains_key(lead_id) {
            continue;
        } else {
            existing_dmids.insert(lead_id.to_string(), true);
        }

        let owner_1_firstname = record.get(*header_map.get("owner_1_firstname").unwrap()).unwrap_or("").trim();
        let owner_1_lastname = record.get(*header_map.get("owner_1_lastname").unwrap()).unwrap_or("").trim();
        let owner_1_name = record.get(*header_map.get("owner_1_name").unwrap()).unwrap_or("").trim();
        let owner_2_firstname = record.get(*header_map.get("owner_2_firstname").unwrap()).unwrap_or("").trim();
        let owner_2_lastname = record.get(*header_map.get("owner_2_lastname").unwrap()).unwrap_or("").trim();
        let owner_2_name = record.get(*header_map.get("owner_2_name").unwrap()).unwrap_or("").trim();

        let fname = if !owner_1_firstname.is_empty() { owner_1_firstname } else { owner_2_firstname };
        let lname = if !owner_1_lastname.is_empty() { owner_1_lastname } else { owner_2_lastname };
        let fullname = if !owner_1_name.is_empty() { owner_1_name } else { owner_2_name };

        if fname.is_empty() {
            continue;
        }

        let street = record.get(*header_map.get("property_address_line_1").unwrap()).unwrap_or("").trim();
        let unit_num = record.get(*header_map.get("property_address_line_2").unwrap()).unwrap_or("").trim();
        let mail_city = record.get(*header_map.get("property_address_city").unwrap()).unwrap_or("").trim();
        let property_state = record.get(*header_map.get("property_address_state").unwrap()).unwrap_or("").trim();
        let zipcode = record.get(*header_map.get("property_address_zipcode").unwrap()).unwrap_or("").trim();
        let latitude = record.get(*header_map.get("property_lat").unwrap()).unwrap_or("").trim();
        let longitude = record.get(*header_map.get("property_lng").unwrap()).unwrap_or("").trim();

        let mailing_address = record.get(*header_map.get("owner_address_line_1").unwrap()).unwrap_or("").trim();
        let mailing_city = record.get(*header_map.get("owner_address_city").unwrap()).unwrap_or("").trim();
        let mailing_state = record.get(*header_map.get("owner_address_state").unwrap()).unwrap_or("").trim();
        let mailing_zip = record.get(*header_map.get("owner_address_zip").unwrap()).unwrap_or("").trim();

        let via = if skip_ai_flag != 0 { 100 } else { 0 };
        let map_image_url = if skip_ai_flag != 0 {
            "google/img/missing.webp".to_string()
        } else {
            "0".to_string()
        };

        let address_record = AddressRecord {
            street: street.to_string(),
            unit_type: "".to_string(),
            unit_num: unit_num.to_string(),
            mail_city: mail_city.to_string(),
            state: property_state.to_string(),
            zip: zipcode.to_string(),
            latitude: latitude.to_string(),
            longitude: longitude.to_string(),
            fullname: fullname.to_string(),
            fname: fname.to_string(),
            lname: lname.to_string(),
            mailing_address: mailing_address.to_string(),
            mailing_city: mailing_city.to_string(),
            mailing_state: mailing_state.to_string(),
            mailing_zip: mailing_zip.to_string(),
            flag: new_flag,
            dmid: lead_id.to_string(),
            via,
            map_image_url,
        };

        // --- Phone number processing with uniqueness check ---
        // Build candidate phone numbers.
        let candidate_phone1 = if !record.get(*header_map.get("contact_1_phone1").unwrap()).unwrap_or("").trim().is_empty() {
            Some(record.get(*header_map.get("contact_1_phone1").unwrap()).unwrap_or("").trim().to_string())
        } else if !record.get(*header_map.get("contact_2_phone1").unwrap()).unwrap_or("").trim().is_empty() {
            Some(record.get(*header_map.get("contact_2_phone1").unwrap()).unwrap_or("").trim().to_string())
        } else {
            None
        };
        let candidate_phone2 = if !record.get(*header_map.get("contact_1_phone2").unwrap()).unwrap_or("").trim().is_empty() {
            Some(record.get(*header_map.get("contact_1_phone2").unwrap()).unwrap_or("").trim().to_string())
        } else if !record.get(*header_map.get("contact_2_phone2").unwrap()).unwrap_or("").trim().is_empty() {
            Some(record.get(*header_map.get("contact_2_phone2").unwrap()).unwrap_or("").trim().to_string())
        } else {
            None
        };
        let candidate_phone3 = if !record.get(*header_map.get("contact_1_phone3").unwrap()).unwrap_or("").trim().is_empty() {
            Some(record.get(*header_map.get("contact_1_phone3").unwrap()).unwrap_or("").trim().to_string())
        } else if !record.get(*header_map.get("contact_2_phone3").unwrap()).unwrap_or("").trim().is_empty() {
            Some(record.get(*header_map.get("contact_2_phone3").unwrap()).unwrap_or("").trim().to_string())
        } else {
            None
        };

        // Combine candidates in order.
        let mut candidates = Vec::new();
        if let Some(p) = candidate_phone1 {
            candidates.push(p);
        }
        if let Some(p) = candidate_phone2 {
            candidates.push(p);
        }
        if let Some(p) = candidate_phone3 {
            candidates.push(p);
        }
        // Filter out phone numbers that already exist (and any empties).
        let unique_candidates: Vec<String> = candidates.into_iter()
            .filter(|p| !p.is_empty() && !global_phone_set.contains(p))
            .collect();

        // If no unique phone numbers, skip the record entirely.
        if unique_candidates.is_empty() {
            continue;
        }

        // Assign final phone numbers from the unique candidates (shifting them over).
        let final_phone1 = unique_candidates.get(0).cloned();
        let final_phone2 = unique_candidates.get(1).cloned();
        let final_phone3 = unique_candidates.get(2).cloned();

        let phone_record = Some(PhoneQueueRecord {
            phone1: final_phone1.clone(),
            phone2: final_phone2.clone(),
            phone3: final_phone3.clone(),
        });

        // Update the global phone set with the new unique numbers.
        if let Some(ref p) = final_phone1 {
            global_phone_set.insert(p.clone());
        }
        if let Some(ref p) = final_phone2 {
            global_phone_set.insert(p.clone());
        }
        if let Some(ref p) = final_phone3 {
            global_phone_set.insert(p.clone());
        }
        // --- End phone number processing ---

        combined_batch.push(CombinedRecord {
            address: address_record,
            phone: phone_record,
        });

        if combined_batch.len() >= batch_size {
            let inserted = process_batch(pool, &mut combined_batch).await
                .context("Failed to process batch")?;
            processed_rows += inserted;
            eprintln!(
                "[{}] Processed batch: {} rows inserted.",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                inserted
            );
        }
    }

    if !combined_batch.is_empty() {
        let inserted = process_batch(pool, &mut combined_batch).await
            .context("Failed to process final batch")?;
        processed_rows += inserted;
        eprintln!(
            "[{}] Processed final batch: {} rows inserted.",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            inserted
        );
    }

    if row_counter >= processed_rows {
        let new_path = Path::new(processed_dir).join(&file_name);
        if file_path.exists() {
            fs::rename(file_path, &new_path)
                .with_context(|| format!("Failed to rename file to {}", new_path.display()))?;
            eprintln!(
                "File {} processed successfully with {} rows inserted.",
                file_name, processed_rows
            );
        } else {
            eprintln!("File {} missing when attempting rename.", file_name);
        }
    } else {
        eprintln!(
            "File {} partially processed. Processed {} out of {} rows. It will be reprocessed.",
            file_name, processed_rows, row_counter
        );
    }

    Ok(())
}

/// Ensures that a campaign exists; creates it if not.
async fn ensure_campaign(
    pool: &Pool<MySql>,
    campaign_name: &str,
) -> Result<(i64, i64)> {
    let row_opt = sqlx::query("SELECT id, flag FROM campaigns WHERE campaignName = ?")
        .bind(campaign_name)
        .fetch_optional(pool)
        .await
        .context("Database query failed for campaigns")?;

    if let Some(row) = row_opt {
        let campaign_id: i64 = row.try_get("id")
            .context("Failed to retrieve campaign ID")?;
        let flag: i64 = row.try_get("flag")
            .context("Failed to retrieve campaign flag")?;
        Ok((campaign_id, flag))
    } else {
        let highest_flag: Option<i64> = sqlx::query_scalar("SELECT MAX(flag) FROM campaigns")
            .fetch_one(pool)
            .await
            .context("Failed to retrieve highest flag from campaigns")?;
        let new_flag = highest_flag.unwrap_or(0) + 1;

        let emoji: Option<String> = sqlx::query_scalar("SELECT e FROM emoji ORDER BY RAND() LIMIT 1")
            .fetch_one(pool)
            .await
            .ok();

        let insert_result = sqlx::query(
            r#"
            INSERT INTO campaigns (campaignName, vertical, textingActive, flag, emoji)
            VALUES (?, 1, 0, ?, ?)
            "#,
        )
        .bind(campaign_name)
        .bind(new_flag)
        .bind(emoji.unwrap_or_default())
        .execute(pool)
        .await
        .context("Failed to insert new campaign")?;

        let campaign_id = insert_result.last_insert_id() as i64;
        Ok((campaign_id, new_flag))
    }
}

/// Pre-fetches existing DMIDs for a given flag.
async fn prefetch_dmids(pool: &Pool<MySql>, flag: i64) -> Result<HashMap<String, bool>> {
    let mut map = HashMap::new();
    let rows = sqlx::query("SELECT DMID FROM address WHERE flag = ?")
        .bind(flag)
        .fetch_all(pool)
        .await
        .context("Failed to fetch existing DMIDs")?;
    for row in rows {
        let dmid: String = row.try_get("DMID")
            .context("Failed to retrieve DMID from row")?;
        map.insert(dmid, true);
    }
    Ok(map)
}

/// Processes a batch of combined records (addresses and optional phone records) in a transaction.
async fn process_batch(
    pool: &Pool<MySql>,
    combined_batch: &mut Vec<CombinedRecord>,
) -> Result<usize> {
    let mut tx = pool.begin().await
        .context("Failed to begin database transaction")?;

    // Bulk insert addresses (note: includes the new state column).
    let mut address_query = String::from(
        "INSERT INTO address (
            street, unit_type, unit_num, mail_city, state, zip, latitude, longitude,
            fullname, fname, lname, mailingAddress, mailingCity, mailingState, mailingZip,
            flag, DMID, via, map_image_url
        ) VALUES ",
    );

    let placeholders: Vec<String> = combined_batch
        .iter()
        .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".to_string())
        .collect();
    address_query += &placeholders.join(", ");

    let mut query = sqlx::query(&address_query);
    for record in combined_batch.iter() {
        let addr = &record.address;
        query = query
            .bind(&addr.street)
            .bind(&addr.unit_type)
            .bind(&addr.unit_num)
            .bind(&addr.mail_city)
            .bind(&addr.state)
            .bind(&addr.zip)
            .bind(&addr.latitude)
            .bind(&addr.longitude)
            .bind(&addr.fullname)
            .bind(&addr.fname)
            .bind(&addr.lname)
            .bind(&addr.mailing_address)
            .bind(&addr.mailing_city)
            .bind(&addr.mailing_state)
            .bind(&addr.mailing_zip)
            .bind(&addr.flag)
            .bind(&addr.dmid)
            .bind(&addr.via)
            .bind(&addr.map_image_url);
    }

    query
        .execute(&mut *tx)
        .await
        .context("Failed to execute bulk insert for addresses")?;

    let last_insert_id: u64 = sqlx::query("SELECT LAST_INSERT_ID()")
        .fetch_one(&mut *tx)
        .await?
        .try_get(0)?;

    // Build bulk insert for phone queues for records with phone data.
    let mut phone_inserts = Vec::new();
    for (i, record) in combined_batch.iter().enumerate() {
        if let Some(phone) = &record.phone {
            let aid = last_insert_id as i64 + i as i64;
            phone_inserts.push((aid, phone));
        }
    }

    if !phone_inserts.is_empty() {
        let mut phone_query = String::from(
            "INSERT INTO phonequeue (aid, phone1, phone2, phone3, step) VALUES ",
        );
        let phone_placeholders: Vec<String> = phone_inserts
            .iter()
            .map(|_| "(?, ?, ?, ?, 11)".to_string())
            .collect();
        phone_query += &phone_placeholders.join(", ");

        let mut phone_query_builder = sqlx::query(&phone_query);
        for (aid, phone) in phone_inserts {
            phone_query_builder = phone_query_builder
                .bind(aid)
                .bind(&phone.phone1)
                .bind(&phone.phone2)
                .bind(&phone.phone3);
        }
        phone_query_builder
            .execute(&mut *tx)
            .await
            .context("Failed to execute bulk insert for phone queues")?;
    }

    tx.commit()
        .await
        .context("Failed to commit database transaction")?;

    let inserted_count = combined_batch.len();
    combined_batch.clear();
    Ok(inserted_count)
}

/// A guard for managing the lock file.
struct LockFileGuard {
    path: String,
}

impl LockFileGuard {
    fn new(path: &str) -> Result<Self> {
        let lock_path = Path::new(path);
        if lock_path.exists() {
            Err(anyhow::anyhow!("Another instance is already running. Exiting."))
        } else {
            fs::write(
                lock_path,
                format!("Process started: {}\n", Local::now().format("%Y-%m-%d %H:%M:%S")),
            )
            .with_context(|| format!("Failed to create lock file at {}", path))?;
            Ok(Self { path: path.to_string() })
        }
    }
}

impl Drop for LockFileGuard {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.path) {
            eprintln!("Failed to remove lock file {}: {:?}", self.path, e);
        }
    }
}
