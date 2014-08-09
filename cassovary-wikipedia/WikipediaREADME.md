# Wikipedia analysis tools using Cassovary

This subproject is aimed at providing tools for analysis of Wikipedia network using Cassovary library.

It provides tools for reading Wikipedia pages graph structure from [Wikipedia Dumps](dumps.wikipedia.org) and
analysing it in Cassovary.

## Parsing Wikipedia dumps

There are three tools to parse Wikipedia dumps:
1. IdsExtractor - extracts page ids writes files containing only page title and page id.
2. RedirectsExtractor - extracts automatic redirections of pages. For example
both *Twitter* and *tweeted* point to the same page: [Twitter](http://en.wikipedia.org/wiki/Twitter).
This means the page has two names, of which *Twitter* is the main one. Redirects extractor
reads Wikipedia dumps and for each page that has more name prints in a single line the main name
and then all the other names that apear in all dump files.
3. DumpToGraphConverter - extracts links between pages and saves them in a adjacency list format
using obfuscated page title as node id.


