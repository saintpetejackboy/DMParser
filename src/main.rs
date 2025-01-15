use anyhow::{Context, Result};
use chrono::Local;
use csv::ReaderBuilder;
use dotenvy::dotenv;
use lazy_static::lazy_static;
use regex::Regex;
use sqlx::{mysql::MySqlPoolOptions, MySql, Pool, Row};
use std::{
    collections::HashMap,
    env,
    fs,  // Removed specific unused imports
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

/// Configuration structure holding all necessary settings.
struct Config {
    database_url: String,
    upload_dir: String,
    processed_dir: String,
    lock_file_path: String,
    batch_size: usize,
    max_execution_seconds: u64,
}

impl Config {
    /// Loads and validates configuration from environment variables.
    fn from_env() -> Result<Self> {
        // Helper function to parse environment variables with validation.
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

/// Processes a single CSV file: parsing, validating, batching inserts,
/// handling errors, and moving the file post-processing.
async fn process_file(
    pool: &Pool<MySql>,
    file_path: &Path,
    processed_dir: &str,
    batch_size: usize,
    max_execution_seconds: u64,
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
            // Move file to processed directory to avoid reprocessing.
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
        .unwrap_or(0); // Default to 0 if parsing fails.
    let original_filename = captures.get(3).unwrap().as_str();

    // Attempt to open the CSV file.
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)
        .with_context(|| format!("Failed to open CSV file: {}", file_name))?;

    // Map headers to their respective indices for quick access.
    let headers = rdr.headers()?.clone();
    let header_map: HashMap<&str, usize> = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| (header.trim(), idx))
        .collect();

    // Define required columns for processing.
    let required_columns = [
        "property_address_line_1",
        "property_address_line_2",
        "property_address_city",
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

    // Check for missing required columns.
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

    // Extract campaign name by stripping the file extension.
    let campaign_name = Path::new(original_filename)
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    // Initialize performance monitoring.
    let start_time = Instant::now();

    // Ensure the campaign exists and retrieve its ID and flag.
    let (_campaign_id, new_flag) = ensure_campaign(pool, &campaign_name).await
        .context("Failed to ensure campaign exists")?;

    // Pre-fetch existing DMIDs to avoid duplicates.
    let mut existing_dmids = prefetch_dmids(pool, new_flag).await
        .context("Failed to prefetch DMIDs")?;

    // Initialize batches for address and phone queue records.
    let mut address_batch: Vec<AddressRecord> = Vec::with_capacity(batch_size);
    let mut phone_queue_batch: Vec<PhoneQueueRecord> = Vec::with_capacity(batch_size);
    let mut row_counter = 0_usize;
    let mut processed_rows = 0_usize;

    // Iterate over each record in the CSV.
    for result in rdr.records() {
        let record = match result {
            Ok(rec) => rec,
            Err(e) => {
                eprintln!("Skipping malformed line in {}: {:?}", file_name, e);
                continue;
            }
        };
        row_counter += 1;

        // Check if the maximum execution time has been exceeded.
        if start_time.elapsed() > Duration::from_secs(max_execution_seconds) {
            eprintln!(
                "Script timeout after {} seconds while processing {}.",
                max_execution_seconds, file_name
            );
            break;
        }

        // Extract lead_id to check for duplicates.
        let lead_id = record.get(*header_map.get("lead_id").unwrap()).unwrap_or("").trim();

        if lead_id.is_empty() {
            // Skip records without a lead_id.
            continue;
        }

        // Skip duplicate DMIDs.
        if existing_dmids.contains_key(lead_id) {
            continue;
        } else {
            existing_dmids.insert(lead_id.to_string(), true);
        }

        // Extract owner information.
        let owner_1_firstname = record.get(*header_map.get("owner_1_firstname").unwrap()).unwrap_or("").trim();
        let owner_1_lastname = record.get(*header_map.get("owner_1_lastname").unwrap()).unwrap_or("").trim();
        let owner_1_name = record.get(*header_map.get("owner_1_name").unwrap()).unwrap_or("").trim();
        let owner_2_firstname = record.get(*header_map.get("owner_2_firstname").unwrap()).unwrap_or("").trim();
        let owner_2_lastname = record.get(*header_map.get("owner_2_lastname").unwrap()).unwrap_or("").trim();
        let owner_2_name = record.get(*header_map.get("owner_2_name").unwrap()).unwrap_or("").trim();

        // Determine first and last names based on availability.
        let fname = if !owner_1_firstname.is_empty() {
            owner_1_firstname
        } else {
            owner_2_firstname
        };
        let lname = if !owner_1_lastname.is_empty() {
            owner_1_lastname
        } else {
            owner_2_lastname
        };
        let fullname = if !owner_1_name.is_empty() {
            owner_1_name
        } else {
            owner_2_name
        };

        // Skip records without a first name.
        if fname.is_empty() {
            continue;
        }

        // Extract address and mailing information.
        let street = record.get(*header_map.get("property_address_line_1").unwrap()).unwrap_or("").trim();
        let unit_num = record.get(*header_map.get("property_address_line_2").unwrap()).unwrap_or("").trim();
        let mail_city = record.get(*header_map.get("property_address_city").unwrap()).unwrap_or("").trim();
        let zipcode = record.get(*header_map.get("property_address_zipcode").unwrap()).unwrap_or("").trim();
        let latitude = record.get(*header_map.get("property_lat").unwrap()).unwrap_or("").trim();
        let longitude = record.get(*header_map.get("property_lng").unwrap()).unwrap_or("").trim();

        let mailing_address = record.get(*header_map.get("owner_address_line_1").unwrap()).unwrap_or("").trim();
        let mailing_city = record.get(*header_map.get("owner_address_city").unwrap()).unwrap_or("").trim();
        let mailing_state = record.get(*header_map.get("owner_address_state").unwrap()).unwrap_or("").trim();
        let mailing_zip = record.get(*header_map.get("owner_address_zip").unwrap()).unwrap_or("").trim();

        // Determine 'via' and 'map_image_url' based on skip_ai_flag.
        let via = if skip_ai_flag != 0 { 100 } else { 0 };
        let map_image_url = if skip_ai_flag != 0 {
            "google/img/missing.webp".to_string()
        } else {
            "0".to_string()
        };

        // Collect address data into the batch.
        address_batch.push(AddressRecord {
            street: street.to_string(),
            unit_type: "".to_string(),
            unit_num: unit_num.to_string(),
            mail_city: mail_city.to_string(),
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
        });

        // Extract phone numbers and collect into the phone queue batch.
        let contact_1_phone1 = record.get(*header_map.get("contact_1_phone1").unwrap()).unwrap_or("").trim();
        let contact_1_phone2 = record.get(*header_map.get("contact_1_phone2").unwrap()).unwrap_or("").trim();
        let contact_1_phone3 = record.get(*header_map.get("contact_1_phone3").unwrap()).unwrap_or("").trim();
        let contact_2_phone1 = record.get(*header_map.get("contact_2_phone1").unwrap()).unwrap_or("").trim();
        let contact_2_phone2 = record.get(*header_map.get("contact_2_phone2").unwrap()).unwrap_or("").trim();
        let contact_2_phone3 = record.get(*header_map.get("contact_2_phone3").unwrap()).unwrap_or("").trim();

        // Prioritize non-empty phone numbers from contact_1 over contact_2.
        let phone1 = if !contact_1_phone1.is_empty() {
            Some(contact_1_phone1.to_string())
        } else if !contact_2_phone1.is_empty() {
            Some(contact_2_phone1.to_string())
        } else {
            None
        };
        let phone2 = if !contact_1_phone2.is_empty() {
            Some(contact_1_phone2.to_string())
        } else if !contact_2_phone2.is_empty() {
            Some(contact_2_phone2.to_string())
        } else {
            None
        };
        let phone3 = if !contact_1_phone3.is_empty() {
            Some(contact_1_phone3.to_string())
        } else if !contact_2_phone3.is_empty() {
            Some(contact_2_phone3.to_string())
        } else {
            None
        };

        if phone1.is_some() || phone2.is_some() || phone3.is_some() {
            phone_queue_batch.push(PhoneQueueRecord { phone1, phone2, phone3 });
        }

        // Once the batch size is reached, process the batch.
        if address_batch.len() >= batch_size {
            let inserted_count = process_batch(pool, &mut address_batch, &mut phone_queue_batch).await
                .context("Failed to process batch")?;
            processed_rows += inserted_count;

            // Log batch processing time and inserted count.
            eprintln!(
                "[{}] Processed batch: {} rows inserted.",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                inserted_count
            );
        }
    }

    // Process any remaining records that didn't fill up a complete batch.
    if !address_batch.is_empty() {
        let inserted_count = process_batch(pool, &mut address_batch, &mut phone_queue_batch).await
            .context("Failed to process final batch")?;
        processed_rows += inserted_count;

        eprintln!(
            "[{}] Processed final batch: {} rows inserted.",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            inserted_count
        );
    }

    // Determine whether the entire file was processed successfully.
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

/// Ensures that a campaign exists in the database. If it doesn't, creates a new one.
/// Returns the campaign ID and its associated flag.
async fn ensure_campaign(
    pool: &Pool<MySql>,
    campaign_name: &str,
) -> Result<(i64, i64)> {
    // Attempt to retrieve the campaign by name.
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
        // If campaign does not exist, create a new one.
        // Determine the new flag by incrementing the current maximum.
        let highest_flag: Option<i64> = sqlx::query_scalar("SELECT MAX(flag) FROM campaigns")
            .fetch_one(pool)
            .await
            .context("Failed to retrieve highest flag from campaigns")?;
        let new_flag = highest_flag.unwrap_or(0) + 1;

        // Select a random emoji for the new campaign.
        let emoji: Option<String> = sqlx::query_scalar("SELECT e FROM emoji ORDER BY RAND() LIMIT 1")
            .fetch_one(pool)
            .await
            .ok();

        // Insert the new campaign into the database.
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

/// Pre-fetches existing DMIDs for a given flag to quickly identify duplicates.
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

/// Processes a batch of address and phone queue records within a single transaction.
/// Utilizes bulk insert operations to optimize performance.
async fn process_batch(
    pool: &Pool<MySql>,
    address_batch: &mut Vec<AddressRecord>,
    phone_queue_batch: &mut Vec<PhoneQueueRecord>,
) -> Result<usize> {
    // Start a new database transaction.
    let mut tx = pool.begin().await
        .context("Failed to begin database transaction")?;

    // Batch insert addresses.
    let mut address_query = String::from(
        "INSERT INTO address (
            street, unit_type, unit_num, mail_city, zip, latitude, longitude,
            fullname, fname, lname, mailingAddress, mailingCity, mailingState, mailingZip,
            flag, DMID, via, map_image_url
        ) VALUES ",
    );

    // Create a list of value placeholders for bulk insert, each as a String.
    let placeholders: Vec<String> = address_batch
        .iter()
        .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".to_string())
        .collect();
    address_query += &placeholders.join(", ");

    let mut query = sqlx::query(&address_query);

    // Bind parameters in the correct order.
    for address in address_batch.iter() {
        query = query
            .bind(&address.street)
            .bind(&address.unit_type)
            .bind(&address.unit_num)
            .bind(&address.mail_city)
            .bind(&address.zip)
            .bind(&address.latitude)
            .bind(&address.longitude)
            .bind(&address.fullname)
            .bind(&address.fname)
            .bind(&address.lname)
            .bind(&address.mailing_address)
            .bind(&address.mailing_city)
            .bind(&address.mailing_state)
            .bind(&address.mailing_zip)
            .bind(&address.flag)
            .bind(&address.dmid)
            .bind(&address.via)
            .bind(&address.map_image_url);
    }

    // Execute the bulk insert for addresses.
    query
        .execute(&mut *tx)
        .await
        .context("Failed to execute bulk insert for addresses")?;

    // Retrieve the last inserted ID to associate phone queues.
    let last_insert_id: u64 = sqlx::query("SELECT LAST_INSERT_ID()")
        .fetch_one(&mut *tx)
        .await?
        .try_get(0)?;

    // Prepare bulk insert for phone queues.
    let mut phone_query = String::from(
        "INSERT INTO phonequeue (aid, phone1, phone2, phone3, step) VALUES ",
    );

    let phone_placeholders: Vec<String> = phone_queue_batch
        .iter()
        .map(|_| "(?, ?, ?, ?, 11)".to_string())
        .collect();
    phone_query += &phone_placeholders.join(", ");

    // Build the final query with bound parameters for each record.
    let mut phone_query_builder = sqlx::query(&phone_query);

    // Aid is sequentially assigned for each phone record, starting at `last_insert_id`.
    // For example, if `address_batch.len() = N`, then addresses get inserted as a block,
    // and the first inserted address is `last_insert_id`, the second is `last_insert_id + 1`, etc.
    //
    // We'll map phonequeue records in the same order they appeared in address_batch.
    // Each phone queue belongs to a matching position in the address_batch. For instance:
    //
    // address_batch[0] => phone_queue_batch[0] => `aid = last_insert_id`
    // address_batch[1] => phone_queue_batch[1] => `aid = last_insert_id + 1`
    //
    // So, each phone record's 'aid' can be computed as `last_insert_id + i` (i = index).
    for (i, phone) in phone_queue_batch.iter().enumerate() {
        let aid = last_insert_id as i64 + i as i64; 
        phone_query_builder = phone_query_builder
            .bind(aid)
            .bind(&phone.phone1)
            .bind(&phone.phone2)
            .bind(&phone.phone3);
    }

    if !phone_queue_batch.is_empty() {
        phone_query_builder
            .execute(&mut *tx)
            .await
            .context("Failed to execute bulk insert for phone queues")?;
    }

    // Commit the transaction to finalize inserts.
    tx.commit()
        .await
        .context("Failed to commit database transaction")?;

    // Return the number of inserted records.
    let inserted_count = address_batch.len();

    // Clear the batches to prepare for the next usage.
    address_batch.clear();
    phone_queue_batch.clear();

    Ok(inserted_count)
}

/// A guard that manages the lifecycle of a lock file.
/// Ensures that the lock file is removed when the guard is dropped (i.e., when the process exits).
struct LockFileGuard {
    path: String,
}

impl LockFileGuard {
    /// Attempts to create a lock file. If it already exists, returns an error.
    fn new(path: &str) -> Result<Self> {
        let lock_path = Path::new(path);
        if lock_path.exists() {
            Err(anyhow::anyhow!(
                "Another instance is already running. Exiting."
            ))
        } else {
            fs::write(
                lock_path,
                format!("Process started: {}\n", Local::now().format("%Y-%m-%d %H:%M:%S")),
            )
            .with_context(|| format!("Failed to create lock file at {}", path))?;
            Ok(Self {
                path: path.to_string(),
            })
        }
    }
}

impl Drop for LockFileGuard {
    /// Removes the lock file when the guard is dropped.
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.path) {
            eprintln!("Failed to remove lock file {}: {:?}", self.path, e);
        }
    }
}
