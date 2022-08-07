use futures::future;
use scraper::{Html, Selector};
use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    println!("Starting");

    // Technically I think we dont need mutexes here because this is async, not threaded
    // But I also think tokio might support using threads along side async, so this is
    // safer, if also more complicated.
    let destinations = Arc::new(Mutex::new(Vec::new()));
    let visited = Arc::new(Mutex::new(HashSet::new()));

    let mut guard = destinations.lock().await;
    guard.push("https://sixey.es".to_string());
    drop(guard);

    let client = reqwest::Client::new();

    let mut jobs = Vec::new();

    for _ in 0..1000 {
        jobs.push(process_queue(
            &client,
            destinations.clone(),
            visited.clone(),
        ));
    }

    if let Err(e) = future::try_join_all(jobs).await {
        println!("{}", e);
    }
    println!("Ended")
}

async fn process_queue(
    client: &reqwest::Client,
    queue: Arc<Mutex<Vec<String>>>,
    visited: Arc<Mutex<HashSet<String>>>,
) -> io::Result<()> {
    loop {
        let mut guard = queue.lock().await;
        if let Some(url) = guard.pop() {
            drop(guard);

            let mut guard = visited.lock().await;
            guard.insert(url.clone());
            drop(guard);

            println!("Begin Request: {}", url);

            if let Ok(resp) = client.get(&url).send().await {
                if let Ok(text) = resp.text().await {
                    let dom = Html::parse_document(&text);

                    // Unwrap safe because static selector is always valid
                    let sel = Selector::parse("a").unwrap();

                    let visited = visited.lock().await;
                    let mut queue = queue.lock().await;
                    for link in dom.select(&sel) {
                        if let Some(link) = link.value().attr("href") {
                            if !visited.contains(link) {
                                if link.starts_with("http") {
                                    queue.push(link.to_string());
                                }
                            }
                        }
                    }
                    drop(visited);
                    drop(queue);
                }
            }
        } else {
            // No work to be done, cede execution
            sleep(Duration::from_nanos(0)).await;
        }
    }
}
