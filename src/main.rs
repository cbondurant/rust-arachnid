mod crawling_engine;
use crawling_engine::CrawlingEngine;

#[tokio::main]
async fn main() {

	// Set up logging
	let subscriber = tracing_subscriber::fmt()
		.compact()
		.with_thread_ids(false)
		.finish();

	if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
		println!("{}", e);
	}
	// Logging started


	tracing::info!("Starting");

	let crawling_engine = CrawlingEngine::new();

	// A site on the corner of the internet, a good starting point.
	crawling_engine.add_destination("https://sixey.es/").await;

	// TODO: find a better way to optimize for the number of workers
	crawling_engine.start_engine(10000).await;

	println!("Ended")
}
