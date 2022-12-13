use flume::{Receiver, Sender};
use sqlx::{Any, Pool};
use std::iter::Iterator;
use std::{collections::HashSet, sync::Arc};
use tokio::signal;
use tokio::sync::{watch, Mutex};

use reqwest::Url;

use tokio::time::{sleep, Duration, Instant};

use scraper::{Html, Selector};

#[derive(Debug)]
pub struct CrawlingEngine {
	client: reqwest::Client,
	pages_input: Sender<Url>,
	pages_output: Receiver<Url>,
	visited: Mutex<HashSet<String>>,
	blockset: HashSet<String>,
	responded: Mutex<i32>,
	db: Pool<Any>,
}

impl CrawlingEngine {
	pub async fn get_visited_count(&self) -> i32 {
		*self.responded.lock().await
	}

	// Is the listed URL blocked by one of our rules?
	// TODO: Improve url parsing and block rules.
	// Reqwest has a url parser, utilize that.
	fn url_is_blocked(&self, url: &Url) -> bool {
		if let Some(host) = url.host() {
			self.blockset.contains(&host.to_string())
		} else {
			false
		}
	}

	pub fn new(pool: Pool<Any>) -> Self {
		Self::with_blockset(pool, HashSet::new())
	}

	pub fn with_blockset(pool: Pool<Any>, blockset: HashSet<String>) -> Self {
		let (rx, tx) = flume::unbounded();
		CrawlingEngine {
			client: reqwest::Client::builder()
				.timeout(Duration::from_secs(15))
				.build()
				.unwrap(),
			pages_input: rx,
			pages_output: tx,
			visited: Mutex::new(HashSet::new()),
			blockset,
			responded: Mutex::new(0),
			db: pool,
		}
	}

	// Loops infinitely with a sleep.
	// Used to report the amount of pages visited and the current page view speed.
	async fn report_statistics(data: Arc<Self>, commands: watch::Receiver<bool>) {
		let start = Instant::now();
		loop {
			if *commands.borrow() {
				break;
			}
			sleep(Duration::from_millis(200)).await;
			let vis_count = data.get_visited_count().await;
			let time_spent = (Instant::now() - start).as_secs_f64();
			let speed = vis_count as f64 / time_spent;
			tracing::info!(
				"{}, {:.1}, {:.1}",
				vis_count,
				(Instant::now() - start).as_secs_f64(),
				speed
			);
		}
	}

	// Begins the async crawling engine, instancing the provided number of workers to process pages.
	pub async fn start_engine(self, workers: i32) -> Self {
		let data = Arc::new(self);

		let (kill_send, kill_recv) = watch::channel(false);

		for _ in 0..workers {
			tokio::spawn(Self::process_queue(Arc::clone(&data), kill_recv.clone()));
		}

		tokio::spawn(Self::report_statistics(
			Arc::clone(&data),
			kill_recv.clone(),
		));
		match signal::ctrl_c().await {
			Ok(()) => {
				kill_send.send(true).unwrap();
			}
			Err(err) => {
				eprintln!("Unable to listen for shutdown signal: {}", err);
				// we also shut down in case of error
			}
		}
		drop(kill_recv);
		while !kill_send.is_closed() {
			sleep(Duration::from_secs(1)).await;
		}
		Arc::try_unwrap(data).unwrap()
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
	#[tracing::instrument(name = "process_queue", skip(data, commands))]
	async fn process_queue(data: Arc<Self>, commands: watch::Receiver<bool>) {
		loop {
			if *commands.borrow() {
				break;
			}

			if let Ok(url) = data.pages_output.recv_async().await {
				if data.url_is_blocked(&url) {
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

				if let Some(hval) = resp
					.headers()
					.get("Content-Type")
					.and_then(|val| val.to_str().ok())
				{
					if !hval.starts_with("text/plain") && !hval.starts_with("text/html") {
						//tracing::info!("Invalid Mimetype {:?}, discarding.", hval);
						continue;
					}
				} else {
					continue;
				}

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

				// Must collect all at once as Html is not Send.
				// As a result we cant still have a reference to HTML
				// In any capacity when we next call an .await
				#[allow(clippy::needless_collect)]
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
