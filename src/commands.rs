use super::scraping::get_words;

use std::collections::HashMap;

use super::crawling_engine::CrawlingEngine;
use sqlx::{Execute, Executor, Row, SqlitePool};

use tokio_stream::StreamExt;

pub async fn construct_idf(pool: &SqlitePool) {
	let query = sqlx::query("SELECT html FROM pages");

	let mut fetch = pool.fetch(query);

	let mut freq_count: HashMap<String, i32> = HashMap::new();

	let mut i: u64 = 0;

	while let Some(row) = fetch.next().await {
		if let Ok(row) = row {
			let text: String = row.try_get(0).unwrap();
			let words = get_words(&text);

			for word in words.split(char::is_whitespace).filter(|s| !s.is_empty()) {
				*freq_count.entry(word.to_owned()).or_default() += 1;
				i += 1;
			}

			tracing::info!("Words processed: {}", i);
		}
	}

	let mut word_pairs: Vec<(&String, &i32)> = freq_count.iter().collect();

	word_pairs.sort_by(|(_, v1), (_, v2)| v1.cmp(v2));

	tracing::info!("Unique: {}, Total:{}", word_pairs.len(), i);

	// Convert frequencies into IDF
	let mut inverse_doc_freq = HashMap::new();

	for (word, freq) in word_pairs {
		inverse_doc_freq
			.entry(word)
			.or_insert_with(|| (i as f64 / *freq as f64).log2());
	}

	tracing::info!("Rewriting keywords table...");
	sqlx::query("DELETE FROM keywords")
		.execute(pool)
		.await
		.unwrap();
	sqlx::query("VACUUM").execute(pool).await.unwrap();

	for (word, frequency) in inverse_doc_freq {
		sqlx::query("INSERT INTO keywords (word, idf) VALUES (?, ?)")
			.bind(word)
			.bind(frequency)
			.execute(pool)
			.await
			.unwrap();
	}
}

pub async fn extract_keywords(pool: &SqlitePool) {
	tracing::info!("Rewriting weights table...");
	sqlx::query("DELETE FROM importance")
		.execute(pool)
		.await
		.unwrap();
	sqlx::query("VACUUM").execute(pool).await.unwrap();

	let mut page = 0;
	let pagesize = 1000;
	loop {
		let query = sqlx::query(
			"SELECT domain, path, html
				FROM pages
				LIMIT ?
				OFFSET ?",
		)
		.bind(pagesize)
		.bind(page * pagesize);
		page += 1;

		let fetch = pool.fetch_all(query).await.unwrap();
		let mut done = true;
		tracing::info!("Rows in page: {}", fetch.len());
		for row in fetch {
			done = false;
			let mut i: u64 = 0;
			let mut freq_count: HashMap<String, i32> = HashMap::new();

			let html: String = row.try_get(2).unwrap();
			let words = get_words(&html);

			for word in words.split(char::is_whitespace).filter(|s| !s.is_empty()) {
				*freq_count.entry(word.to_owned()).or_default() += 1;
				i += 1;
			}

			let mut tf: Vec<(String, f64)> = freq_count
				.into_iter()
				.map(|(term, freq)| (term, freq as f64 / i as f64))
				.collect();

			// Sort largest first.
			tf.sort_by(|(_, weight1), (_, weight2)| (*weight2).partial_cmp(weight1).unwrap());

			for (word, weight) in tf.into_iter().take(10) {
				sqlx::query(
					"INSERT INTO importance(domain, path, word, weight) VALUES (?, ?, ?, ?)",
				)
				.bind(row.get::<String, usize>(0))
				.bind(row.get::<String, usize>(1))
				.bind(word)
				.bind(weight)
				.execute(pool)
				.await
				.unwrap();
			}
		}
		tracing::info!("Pages done: {}", page);
		if done {
			break;
		}
	}
}

pub async fn crawl_pages(pool: SqlitePool, origins: Vec<reqwest::Url>) {
	let crawling_engine = CrawlingEngine::new(pool);

	// A site on the corner of the internet, a good starting point.
	// unwrapping because I know its a real addres.
	for page in origins {
		crawling_engine.add_destination(page).await;
	}

	// TODO: find a better way to optimize for the number of workers
	let crawling_engine = crawling_engine.start_engine(1000).await;

	tracing::info!(
		"Total Pages visited: {}",
		crawling_engine.get_visited_count().await
	);
}

pub async fn search_keywords(pool: &SqlitePool, keywords: Vec<String>) -> Vec<String> {
	let mut binding = Vec::new();
	for _ in 0..keywords.len() {
		binding.push("?");
	}

	let binding = binding.join(",");

	let querystring = format!(
		"SELECT domain || path as URL, TOTAL(weight) as total_weight
		FROM importance
		WHERE lower(word) in ({})
		GROUP BY URL
		ORDER BY total_weight DESC
		LIMIT 200",
		binding
	);

	let mut query = sqlx::query(&querystring);

	for keyword in keywords {
		query = query.bind(keyword);
	}

	let query = query.fetch_all(pool).await.unwrap();

	let results: Vec<String> = query.into_iter().map(|row| row.get(0)).collect();
	results
}
