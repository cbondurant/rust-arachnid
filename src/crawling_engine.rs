use flume::{Receiver, Sender};
use futures::future::{self};
use sqlx::{Pool, Sqlite};
use std::iter::Iterator;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex;

use reqwest::Url;

use tokio::time::{sleep, Duration, Instant};

use scraper::{Html, Selector};

pub struct CrawlingEngine {
	client: reqwest::Client,
	pages_input: Sender<Url>,
	pages_output: Receiver<Url>,
	visited: Mutex<HashSet<String>>,
	blocklist: Vec<String>,
	responded: Mutex<i32>,
	db: Pool<Sqlite>,
}

impl CrawlingEngine {
	pub async fn get_visited_count(&self) -> i32 {
		*self.responded.lock().await
	}

	// Is the listed URL blocked by one of our rules?
	// TODO: Improve url parsing and block rules.
	// Reqwest has a url parser, utilize that.
	fn url_is_blocked(&self, url: &str) -> bool {
		for page in self.blocklist.iter() {
			if url.contains(page.as_str()) {
				return true;
			}
		}
		false
	}

	pub fn new(pool: Pool<Sqlite>) -> Self {
		let (rx, tx) = flume::unbounded();
		CrawlingEngine {
			client: reqwest::Client::new(),
			pages_input: rx,
			pages_output: tx,
			visited: Mutex::new(HashSet::new()),
			blocklist: Vec::new(),
			responded: Mutex::new(0),
			db: pool,
		}
	}

	// Loops infinitely with a sleep.
	// Used to report the amount of pages visited and the current page view speed.
	async fn report_statistics(data: Arc<Self>) {
		let start = Instant::now();
		loop {
			sleep(Duration::from_millis(200)).await;
			let vis_count = data.get_visited_count().await;
			let time_spent = (Instant::now() - start).as_secs_f64();
			let speed = vis_count as f64 / time_spent;
			tracing::info!(
				"{}, {}, {}",
				vis_count,
				(Instant::now() - start).as_secs_f64(),
				speed
			);
		}
	}

	// Begins the async crawling engine, instancing the provided number of workers to process pages.
	pub async fn start_engine(self, workers: i32) {
		let mut jobs = Vec::new();

		let data = Arc::new(self);

		for _ in 0..workers {
			jobs.push(tokio::spawn(Self::process_queue(Arc::clone(&data))));
		}

		tokio::join!(
			Self::report_statistics(Arc::clone(&data)),
			future::join_all(jobs)
		);
	}

	// Add a single destination to the queue of destinations to crawl.
	pub async fn add_destination(&self, destination: Url) {
		self.pages_input.send_async(destination).await.unwrap();
	}

	// Adds every destination inthe provided iterator to the queue of destinations to crawl
	pub async fn add_destinations<'a, I>(&self, destinations: I)
	where
		I: Iterator<Item = Url>,
	{
		for dest in destinations {
			self.pages_input.send_async(dest).await.unwrap();
		}
	}

	// Worker thread, intended to be instance in paralell, reads from the queue endlessly.
	async fn process_queue(data: Arc<Self>) {
		loop {
			if let Ok(url) = data.pages_output.recv_async().await {
				if data.url_is_blocked(url.as_str()) {
					continue;
				}

				let mut visited_guard = data.visited.lock().await;
				let key = url.domain().unwrap_or("").to_owned() + url.path();
				if visited_guard.contains(&key) {
					continue;
				}
				visited_guard.insert(key);
				drop(visited_guard);

				//tracing::info!("Begin Request: {}", url);

				let resp = match data.client.get(url.clone()).send().await {
					Ok(resp) => resp,
					Err(_e) => {
						//tracing::info!("{}", e);
						continue;
					}
				};

				let text = match resp.text().await {
					Ok(resp) => resp,
					Err(e) => {
						tracing::error!("{}", e);
						continue;
					}
				};

				match sqlx::query("INSERT INTO pages (domain, path, html) VALUES (?, ?, ?)")
					.bind(url.domain())
					.bind(url.path())
					.bind(text.clone())
					.execute(&data.db)
					.await
				{
					Ok(_) => (),
					Err(e) => tracing::warn!("{}", e),
				};

				// Unwrap safe because static selector is always valid
				let sel = Selector::parse("a").unwrap();

				let visited = data.visited.lock().await;
				let destinations: Vec<Url> = Html::parse_document(&text)
					.select(&sel)
					.filter_map(|e| e.value().attr("href"))
					.filter_map(|s| Url::parse(s).ok())
					.filter(|url| {
						let key = url.domain().unwrap_or("").to_owned() + url.path();
						!visited.contains(&key)
							&& (url.scheme() == "http" || url.scheme() == "https")
					})
					.collect();
				data.add_destinations(destinations.into_iter()).await;
				let mut resp = data.responded.lock().await;
				*resp += 1;
				drop(resp);
				drop(visited);
			} else {
				// No work to be done, cede execution
				sleep(Duration::from_nanos(1)).await;
			}
		}
	}
}
