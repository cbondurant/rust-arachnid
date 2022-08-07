use futures::future;
use std::collections::HashSet;
use std::iter::Iterator;
use tokio::sync::Mutex;

use tokio::time::{sleep, Duration, Instant};

use scraper::{Html, Selector};

#[derive(Debug)]
pub struct CrawlingEngine {
	client: reqwest::Client,
	queue: Mutex<Vec<String>>,
	visited: Mutex<HashSet<String>>,
	blocklist: Vec<String>,
}

impl CrawlingEngine {

	#[tracing::instrument]
	pub async fn get_visited_count(&self) -> usize{
		self.visited.lock().await.len()
	}

	fn url_is_blocked(&self, url: &str) -> bool {
		for page in self.blocklist.iter() {
			if url.contains(page.as_str()) {
				return true;
			}
		}
		false
	}

	pub fn new() -> Self {
		CrawlingEngine {
			client: reqwest::Client::new(),
			queue: Mutex::new(Vec::new()),
			visited: Mutex::new(HashSet::new()),
			blocklist: Vec::new(),
		}
	}

	async fn report_statistics(&self){
		let start = Instant::now();
		loop{
			sleep(Duration::from_secs(3)).await;
			let vis_count = self.get_visited_count().await;
			let time_spent = (Instant::now() - start).as_secs_f64();
			let speed = vis_count as f64/ time_spent;
			tracing::info!("{}, {}, {}", vis_count, (Instant::now() - start).as_secs_f64(),speed);
		}
	}

	#[tracing::instrument]
	pub async fn start_engine(&self, workers: i32) {
		let mut jobs = Vec::new();

		for _ in 0..workers {
			jobs.push(self.process_queue());
		}


		tokio::join!(self.report_statistics(), future::join_all(jobs));
	}

	#[tracing::instrument]
	pub async fn add_destination(&self, destination: &str) {
		let mut queue = self.queue.lock().await;
		queue.push(destination.to_string());
		drop(queue);
	}

	pub async fn add_destinations<'a, I>(&self, destinations: I)
	where
		I: Iterator<Item = &'a str>,
	{
		let mut queue = self.queue.lock().await;
		for dest in destinations {
			queue.push(dest.to_string());
		}
		drop(queue);
	}

	#[tracing::instrument]
	async fn process_queue(&self) -> reqwest::Result<()> {
		loop {
			let mut queue_guard = self.queue.lock().await;
			if let Some(url) = queue_guard.pop() {
				drop(queue_guard);

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
						tracing::warn!("{}", e);
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
