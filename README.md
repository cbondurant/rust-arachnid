A full rewrite of my test webcrawler "arachnid" in rust.

This can currently achieve a peak of around 700 pages per second on my machine.

Todo:

Start storing pages and metadata regarding pages.
Runtime configuration.
Crawl resumption.
Improving the blocklisting system for domains and pages I dont want to crawl.