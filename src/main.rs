#![allow(unreachable_code)]
mod crawling_engine;
mod scraping;

use scraping::get_words;
use tokio::time::sleep;

use std::str::FromStr;
use std::{collections::HashMap, time::Duration};

use crawling_engine::CrawlingEngine;
use sqlx::{
	sqlite::{
		SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode, SqlitePoolOptions,
		SqliteSynchronous,
	},
	Executor, Row, SqlitePool,
};

use tokio_stream::StreamExt;

async fn ensure_database(path: &str) -> SqlitePool {
	tracing::info!("Opening Database");

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

	if let Err(e) = sqlx::query("PRAGMA foreign_keys = ON").execute(&pool).await {
		tracing::warn!("{:?}", e);
	}

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
		Err(sqlx::Error::Database(e)) => match e.code() {
			Some(c) => {
				if c != "1" {
					tracing::warn!("{}", e);
				}
			}
			None => tracing::warn!("{}", e),
		},
		Err(e) => tracing::warn!("{:?}", e),
	}

	match sqlx::query(
		"CREATE TABLE keywords(
	word TEXT NOT NULL,
	idf REAL NOT NULL,
	PRIMARY KEY(word))",
	)
	.execute(&pool)
	.await
	{
		Ok(_) => (),
		Err(sqlx::Error::Database(e)) => match e.code() {
			Some(c) => {
				if c != "1" {
					tracing::warn!("{}", e);
				}
			}
			None => tracing::warn!("{}", e),
		},
		Err(e) => tracing::warn!("{}", e),
	}

	match sqlx::query(
		"CREATE TABLE importance (
			domain	TEXT NOT NULL,
			path	TEXT NOT NULL,
			word	TEXT NOT NULL,
			weight	REAL NOT NULL,
			FOREIGN KEY(domain, path) REFERENCES pages(domain, path) ON DELETE CASCADE,
			PRIMARY KEY(path, domain, word),
			FOREIGN KEY(word) REFERENCES keywords(word) ON DELETE CASCADE
		);",
	)
	.execute(&pool)
	.await
	{
		Ok(_) => (),
		Err(sqlx::Error::Database(e)) => match e.code() {
			Some(c) => {
				if c != "1" {
					tracing::warn!("{}", e);
				}
			}
			None => tracing::warn!("{}", e),
		},
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

			tracing::info!("Words processed: {}", i);
		}
	}

	let mut word_pairs: Vec<(&String, &i32)> = freq_count.iter().collect();

	word_pairs.sort_by(|(_, v1), (_, v2)| v1.cmp(v2));

	tracing::info!("Unique: {}, Total:{}", word_pairs.len(), i);

	// Convert frequencies into IDF
	let mut inverse_doc_freq = HashMap::new();

	for (word, freq) in word_pairs {
		inverse_doc_freq
			.entry(word)
			.or_insert_with(|| (i as f64 / *freq as f64).log2());
	}

	tracing::info!("Rewriting keywords table...");
	sqlx::query("DELETE FROM keywords")
		.execute(pool)
		.await
		.unwrap();
	sqlx::query("VACUUM").execute(pool).await.unwrap();

	for (word, frequency) in inverse_doc_freq {
		sqlx::query("INSERT INTO keywords (word, idf) VALUES (?, ?)")
			.bind(word)
			.bind(frequency)
			.execute(pool)
			.await
			.unwrap();
	}
}

async fn extract_keywords(pool: &SqlitePool) {
	tracing::info!("Rewriting weights table...");
	sqlx::query("DELETE FROM importance")
		.execute(pool)
		.await
		.unwrap();
	sqlx::query("VACUUM").execute(pool).await.unwrap();

	let mut page = 0;
	let pagesize = 1000;
	loop {
		let query = sqlx::query(
			"SELECT domain, path, html
				FROM pages
				LIMIT ?
				OFFSET ?",
		)
		.bind(pagesize)
		.bind(page * pagesize);
		page += 1;

		let fetch = pool.fetch_all(query).await.unwrap();
		let mut done = true;
		tracing::info!("Rows in page: {}", fetch.len());
		for row in fetch {
			done = false;
			let mut i: u64 = 0;
			let mut freq_count: HashMap<String, i32> = HashMap::new();

			let html: String = row.try_get(2).unwrap();
			let words = get_words(&html);

			for word in words.split(char::is_whitespace).filter(|s| !s.is_empty()) {
				*freq_count.entry(word.to_owned()).or_default() += 1;
				i += 1;
			}

			let mut tf: Vec<(String, f64)> = freq_count
				.into_iter()
				.map(|(term, freq)| (term, freq as f64 / i as f64))
				.collect();

			// Sort largest first.
			tf.sort_by(|(_, weight1), (_, weight2)| (*weight2).partial_cmp(weight1).unwrap());

			for (word, weight) in tf.into_iter().take(10) {
				sqlx::query(
					"INSERT INTO importance(domain, path, word, weight) VALUES (?, ?, ?, ?)",
				)
				.bind(row.get::<String, usize>(0))
				.bind(row.get::<String, usize>(1))
				.bind(word)
				.bind(weight)
				.execute(pool)
				.await
				.unwrap();
			}
		}
		tracing::info!("Pages done: {}", page);
		if done {
			break;
		}
	}
}

async fn crawl_pages(pool: SqlitePool, origins: Vec<reqwest::Url>) {
	let crawling_engine = CrawlingEngine::new(pool);

	// A site on the corner of the internet, a good starting point.
	// unwrapping because I know its a real addres.
	for page in origins {
		crawling_engine.add_destination(page).await;
	}

	// TODO: find a better way to optimize for the number of workers
	let crawling_engine = crawling_engine.start_engine(1000).await;

	tracing::info!(
		"Total Pages visited: {}",
		crawling_engine.get_visited_count().await
	);
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
		tracing::error!("{}", e);
	}

	tracing::info!("Starting");

	let mut path = String::from("sqlite://./db.sqlite");
    let mut origins = Vec::new();

	let args: Vec<String> = std::env::args().collect();
	if args.len() >= 3 {
		let mut arg_iter = args.iter().skip(2);
		while let Some(arg) = arg_iter.next() {
			if arg == "--db" {
				path = match arg_iter.next() {
					Some(path) => format!("sqlite://{}", path),
					None => {
						eprintln!("Argument --db provided but no path given!");
						return;
					}
				}
			}else{
                origins.push(match reqwest::Url::parse(arg) {
                    Ok(url) => url,
                    Err(e) => {
                        eprintln!("Invalid url! {:?}" , e);
                        return;
                    }
                })
            }
		}
	}

	println!("Opening: {}", path);
	sleep(Duration::from_secs_f64(0.5)).await;

	let pool = ensure_database(path.as_str()).await;

	match args[1].to_lowercase().as_str() {
		"crawl" => crawl_pages(pool.clone(), origins).await,
		"process" => construct_idf(&pool).await,
		"extract" => extract_keywords(&pool).await,
		arg => tracing::warn!(
			"Invalid Command \"{}\", valid commands are \"crawl\" and \"process\"",
			arg
		),
	}
}
