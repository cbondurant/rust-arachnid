use flume::{Receiver, Sender};
use futures::future;
use std::collections::HashSet;
use std::iter::Iterator;
use tokio::sync::Mutex;

use tokio::time::{sleep, Duration, Instant};

use scraper::{Html, Selector};

pub struct CrawlingEngine {
	client: reqwest::Client,
	pages_input: Sender<String>,
	pages_output: Receiver<String>,
	visited: Mutex<HashSet<String>>,
	blocklist: Vec<String>,
}

impl CrawlingEngine {
	pub async fn get_visited_count(&self) -> usize {
		self.visited.lock().await.len()
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

	pub fn new() -> Self {
		let (rx, tx) = flume::unbounded();
		CrawlingEngine {
			client: reqwest::Client::new(),
			pages_input: rx,
			pages_output: tx,
			visited: Mutex::new(HashSet::new()),
			blocklist: Vec::new(),
		}
	}

	// Loops infinitely with a sleep.
	// Used to report the amount of pages visited and the current page view speed.
	async fn report_statistics(&self) {
		let start = Instant::now();
		loop {
			sleep(Duration::from_millis(200)).await;
			let vis_count = self.get_visited_count().await;
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
	pub async fn start_engine(&self, workers: i32) {
		let mut jobs = Vec::new();

		for _ in 0..workers {
			jobs.push(self.process_queue());
		}

		tokio::join!(self.report_statistics(), future::join_all(jobs));
	}

	// Add a single destination to the queue of destinations to crawl.
	pub async fn add_destination(&self, destination: &str) {
		self.pages_input
			.send_async(destination.to_string())
			.await
			.unwrap();
	}

	// Adds every destination inthe provided iterator to the queue of destinations to crawl
	pub async fn add_destinations<'a, I>(&self, destinations: I)
	where
		I: Iterator<Item = &'a str>,
	{
		for dest in destinations {
			self.pages_input.send_async(dest.to_string()).await.unwrap();
		}
	}

	// Worker thread, intended to be instance in paralell, reads from the queue endlessly.
	async fn process_queue(&self) -> reqwest::Result<()> {
		loop {
			if let Ok(url) = self.pages_output.recv_async().await {
				if self.url_is_blocked(url.as_str()) {
					continue;
				}

				let mut visited_guard = self.visited.lock().await;
				if visited_guard.contains(&url) {
					continue;
				}
				visited_guard.insert(url.clone());
				drop(visited_guard);

				//tracing::info!("Begin Request: {}", url);

				let resp = match self.client.get(&url).send().await {
					Ok(resp) => resp,
					Err(e) => {
						tracing::info!("{}", e);
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

				let dom = Html::parse_document(&text);

				// Unwrap safe because static selector is always valid
				let sel = Selector::parse("a").unwrap();

				let visited = self.visited.lock().await;
				self.add_destinations(
					dom.select(&sel)
						.filter_map(|e| e.value().attr("href"))
						.filter(|link| {
							!visited.contains(&link.to_string()) && link.starts_with("http")
						}),
				)
				.await;

				drop(visited);
			} else {
				// No work to be done, cede execution
				sleep(Duration::from_nanos(1)).await;
			}
		}
	}
}
