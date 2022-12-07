mod crawling_engine;

use std::str::FromStr;

use crawling_engine::CrawlingEngine;
use sqlx::sqlite::{
	SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode, SqlitePoolOptions,
	SqliteSynchronous,
};

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

	let sqlite_options = SqliteConnectOptions::from_str("sqlite://./db.sqlite")
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

	let crawling_engine = CrawlingEngine::new(pool);

	// A site on the corner of the internet, a good starting point.
	// unwrapping because I know its a real addres.
	crawling_engine
		.add_destination(reqwest::Url::parse("http://sixey.es/").unwrap())
		.await;

	// TODO: find a better way to optimize for the number of workers
	let crawling_engine = crawling_engine.start_engine(1000).await;

	println!("{}", crawling_engine.get_visited_count().await);

	println!("Ended")
}
