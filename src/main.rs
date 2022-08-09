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

	let mut crawling_engine = CrawlingEngine::new();

	// A site on the corner of the internet, a good starting point.
	// unwrapping because I know its a real addres.
	crawling_engine.add_destination(reqwest::Url::parse("https://sixey.es/").unwrap()).await;

	crawling_engine.register_callback(|url,html| {
		// Add info here
	});

	// TODO: find a better way to optimize for the number of workers
	crawling_engine.start_engine(10000).await;

	println!("Ended")
}
