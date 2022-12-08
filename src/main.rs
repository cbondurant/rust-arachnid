#![allow(unreachable_code)]
mod crawling_engine;
mod scraping;

use scraping::get_words;
use sqlx::SqlitePool;

use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use std::{collections::HashMap, fs::File};

use crawling_engine::CrawlingEngine;
use sqlx::{
	sqlite::{
		SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode, SqlitePoolOptions,
		SqliteSynchronous,
	},
	Executor, Row,
};

use tokio_stream::StreamExt;

async fn ensure_database(path: &str) -> SqlitePool {
	let sqlite_options = SqliteConnectOptions::from_str(path)
		.unwrap()
		.create_if_missing(true)
		.synchronous(SqliteSynchronous::Off)
		.locking_mode(SqliteLockingMode::Exclusive)
		.journal_mode(SqliteJournalMode::Wal);

	let pool = SqlitePoolOptions::new()
		//.max_connections(10000)
		.max_connections(1)
		.test_before_acquire(false)
		.connect_with(sqlite_options)
		.await
		.unwrap();

	match sqlx::query(
		"CREATE TABLE pages(
	domain TEXT NOT NULL,
	path TEXT NOT NULL,
	html TEXT NOT NULL,
	PRIMARY KEY(domain, path))",
	)
	.execute(&pool)
	.await
	{
		Ok(_) => (),
		Err(e) => tracing::warn!("{}", e),
	}

	match sqlx::query(
		"CREATE TABLE keywords(
	word TEXT NOT NULL,
	IDF REAL NOT NULL,
	PRIMARY KEY(word))",
	)
	.execute(&pool)
	.await
	{
		Ok(_) => (),
		Err(e) => tracing::warn!("{}", e),
	}

	pool
}

async fn construct_idf(pool: &SqlitePool) {
	let query = sqlx::query("SELECT html FROM pages");

	let mut fetch = pool.fetch(query);

	let mut freq_count: HashMap<String, i32> = HashMap::new();

	let mut i: u64 = 0;

	while let Some(row) = fetch.next().await {
		if let Ok(row) = row {
			let text: String = row.try_get(0).unwrap();
			let words = get_words(&text);

			for word in words.split(char::is_whitespace).filter(|s| !s.is_empty()) {
				*freq_count.entry(word.to_owned()).or_default() += 1;
				i += 1;
			}

			println!("{}", i);
		}
	}

	let mut word_pairs: Vec<(&String, &i32)> = freq_count.iter().collect();

	word_pairs.sort_by(|(_, v1), (_, v2)| v1.cmp(v2));

	println!("Unique: {}, Total:{}", word_pairs.len(), i);

	for word in word_pairs.iter().rev().take(10) {
		println!("{:?}", word);
	}

	// Convert frequencies into IDF
	let mut inverse_doc_freq = HashMap::new();

	for (word, freq) in word_pairs {
		inverse_doc_freq
			.entry(word)
			.or_insert_with(|| (i as f64 / *freq as f64).log2());
	}

	let serialize = serde_json::to_string(&inverse_doc_freq).unwrap();

	let mut file = File::create(Path::new("./inverse_doc_freq")).unwrap();

	file.write_all(serialize.as_bytes()).unwrap();
}

async fn crawl_pages(pool: SqlitePool) {
	let crawling_engine = CrawlingEngine::new(pool);

	// A site on the corner of the internet, a good starting point.
	// unwrapping because I know its a real addres.
	crawling_engine
		.add_destination(reqwest::Url::parse("http://sixey.es/").unwrap())
		.await;

	// TODO: find a better way to optimize for the number of workers
	let crawling_engine = crawling_engine.start_engine(1000).await;

	println!("{}", crawling_engine.get_visited_count().await);
}

#[tokio::main]
async fn main() {
	// Set up logging
	let subscriber = tracing_subscriber::fmt()
		.compact()
		.without_time()
		.with_file(true)
		.with_line_number(true)
		.with_thread_ids(false)
		.with_target(false)
		.finish();

	if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
		println!("{}", e);
	}
	// Logging started

	tracing::info!("Starting");

	let pool = ensure_database("sqlite://./db.sqlite").await;

	crawl_pages(pool.clone()).await;

	construct_idf(&pool).await;
}
