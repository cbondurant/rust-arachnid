use std::collections::HashSet;

use html5ever::tendril::{Tendril, TendrilSink};
use kuchiki::{parse_html, NodeRef};

fn remove_unwanted_tags(node: &NodeRef) {
	let mut disallowed_tags = HashSet::new();

	disallowed_tags.insert("script");
	disallowed_tags.insert("head");
	for child in node.children() {
		match child.0.data() {
			kuchiki::NodeData::Element(e) => {
				if disallowed_tags.contains(e.name.local.to_string().as_str()) {
					child.detach()
				} else {
					remove_unwanted_tags(&child);
				}
			}
			_ => (),
		}
	}
	println!("Walking out!");
}

pub fn get_words(text: &str) -> String {
	let tendril = Tendril::from(text);
	let doc = parse_html().one(tendril);

	remove_unwanted_tags(&doc);

	doc.text_contents()
}

#[cfg(test)]
mod testing {
	use super::get_words;

	#[test]
	fn get_words_removes_unwanted_tags() {
		let test = "<!DOCTYPE html>
		<html>
		<head>
		<title>Also dont include titles</title>
		<!-- comment test -->
		</head>

		<body><script> Dont include this</script>These are real words though.</body></html>";

		assert_eq!(
			get_words(test).trim(),
			String::from("These are real words though.")
		);
	}
}
