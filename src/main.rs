#![allow(unreachable_code)]
mod commands;
mod crawling_engine;
mod scraping;

use clap::{command, Parser, Subcommand};
use reqwest::Url;
use tokio::time::sleep;
use tracing::Level;

use std::str::FromStr;
use std::time::Duration;

use sqlx::{
	sqlite::{
		SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode, SqlitePoolOptions,
		SqliteSynchronous,
	},
	SqlitePool,
};

use commands::*;

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

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
	#[command(subcommand)]
	command: Command,

	#[arg(long, default_value_t = String::from("./db.sqlite"))]
	db: String,

	#[arg(long, short, default_value_t = Level::INFO)]
	logging_level: Level,
}

#[derive(Subcommand, Debug)]
enum Command {
	Crawl { origins: Vec<Url> },
	Process,
	Extract,
	Search { keywords: Vec<String> },
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

	let args = Args::parse();

	let path = String::from(format!("sqlite://{}", args.db));
	println!("Opening: {}", path);
	sleep(Duration::from_secs_f64(0.5)).await;

	let pool = ensure_database(path.as_str()).await;

	match args.command {
		Command::Crawl { origins } => crawl_pages(pool.clone(), origins).await,
		Command::Process => construct_idf(&pool).await,
		Command::Extract => extract_keywords(&pool).await,
		Command::Search { keywords } => {
			for url in search_keywords(&pool, keywords).await {
				println!("{}", url);
			}
		}
	}
}
