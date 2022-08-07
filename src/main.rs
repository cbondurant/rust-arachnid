mod crawling_engine;
use crawling_engine::CrawlingEngine;

#[tokio::main]
async fn main() {
	let subscriber = tracing_subscriber::fmt()
		.compact()
		.with_thread_ids(false)
		.finish();

	if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
		println!("{}", e);
	}

	tracing::info!("Starting");

	let crawling_engine = CrawlingEngine::new();

	crawling_engine.add_destination("https://sixey.es/").await;

	crawling_engine.start_engine(10000).await;

	println!("Ended")
}
