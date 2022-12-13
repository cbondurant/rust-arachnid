A web crawler and basic search engine, all controllable from the command line.

This can currently achieve a peak of around 300 pages per second on my machine and internet.

Todo:

- Runtime configuration.
- Crawl resumption.
- Improving the blocklisting system for domains and pages I dont want to crawl.

# How to use, the basics

build with `cargo build -r`

run the following in order:

All of these commands take an additional argument `--db <PATH>` which provides a explicit path for the sql database. If not provided this defaults to `./db.sqlite`

- `arachnid crawl <origins>` to begin crawling webpages. End with ctrl+C and allow it to commit everything to the sqlite database. origins is a list of complete urls used to begin the crawling at. (Crawl resumption does not currently exist, so a new crawl must be done every invocation)
- `arachnid process` Takes the existing sqlite database and calculates Inverse Document Frequency for all unique printable words across all documents crawled.
- `arachnid extract` Calcuates document frequency and stores Tf-Idf values in a third table of the sql database.
- `arachnid search <keywords>` does a basic TF-IDF rank search for the terms across the document.

In some situations this readme may not be up to date with the current state of the tool. `arachnid -h` should be more accurate.

At current time only explicit sql queries can be used to query the resulting term search database, however the most important words for each document should be stored in the `importance` table, where a larger weight value represents greater importance of that word for the linked document.


