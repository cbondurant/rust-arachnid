mod crawling_engine;

use std::{str::FromStr, time::Duration};

use crawling_engine::CrawlingEngine;
use futures::{future::BoxFuture, FutureExt};
use reqwest::Url;
use sqlx::{
	sqlite::{
		SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode, SqlitePoolOptions,
		SqliteSynchronous,
	},
	SqlitePool,
};

fn store_location<'a>(pool: SqlitePool, url: &'a Url, html: &'a str) -> BoxFuture<'a, ()> {
	async move {
		match sqlx::query("INSERT INTO pages (domain, path, html) VALUES (?, ?, ?)")
			.bind(url.domain())
			.bind(url.path())
			.bind(html)
			.execute(&pool)
			.await
		{
			Ok(_) => (),
			Err(e) => tracing::warn!("{}", e),
		};
	}
	.boxed()
}

#[tokio::main]
async fn main() {
	// Set up logging
	let subscriber = tracing_subscriber::fmt()
		.compact()
		.without_time()
		.with_thread_ids(false)
		.finish();

	if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
		println!("{}", e);
	}
	// Logging started

	tracing::info!("Starting");

	let mut crawling_engine = CrawlingEngine::new();

	// A site on the corner of the internet, a good starting point.
	// unwrapping because I know its a real addres.
	crawling_engine
		.add_destination(reqwest::Url::parse("http://sixey.es/").unwrap())
		.await;

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
		.acquire_timeout(Duration::new(60, 0))
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

	crawling_engine.register_callback(Box::new(move |url, html| {
		store_location(pool.clone(), url, html)
	}));

	// TODO: find a better way to optimize for the number of workers
	crawling_engine.start_engine(10000).await;

	println!("Ended")
}
