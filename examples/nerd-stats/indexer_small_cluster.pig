/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 *
 * @params $DIR - the directory where the files should be stored
 *         $STOPLIST_PATH - the location of the stoplist in HDFS		
 *         $STOPLIST_NAME - the filename of the stoplist
 *         $INPUT - the wikipedia XML dump
 *         $MIN_COUNT - the minumum count for a token to be included in the index
 *         $PIGNLPROC_JAR - the location of the pignlproc jar
 *         $LANG - the language of the Wikidump 
 *         $MAX_SPAN_LENGTH - the maximum length (in chars) for a paragraph span
 */


-- TEST: set parallelism level for reducers
SET default_parallel 15;

SET job.name 'Wikipedia-Token-Counts-per-URI for $LANG';

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Define alias for tokenizer function
DEFINE tokens pignlproc.index.GetCountsLucene('$STOPLIST_PATH', '$STOPLIST_NAME');
DEFINE textWithLink pignlproc.evaluation.ParagraphsWithLink('$MAX_SPAN_LENGTH');

--------------------
-- prepare
--------------------

-- Parse the wikipedia dump and extract text and links data
parsed = LOAD '$INPUT'
  USING pignlproc.storage.ParsingWikipediaLoader('$LANG')
  AS (title, id, pageUrl, text, redirect, links, headers, paragraphs);


-- filter as early as possible
SPLIT parsed INTO 
  parsedRedirects IF redirect IS NOT NULL,
  parsedNonRedirects IF redirect IS NULL;

-- Project articles
articles = FOREACH parsedNonRedirects GENERATE
  pageUrl,
  text,
  links,
  paragraphs;

-- Extract paragraph contexts of the links 
paragraphs = FOREACH articles GENERATE
  pageUrl,
  FLATTEN(textWithLink(text, links, paragraphs))
  AS (paragraphIdx, paragraph, targetUri, startPos, endPos);

--Changes for indexing on small cluster
contexts = FOREACH paragraphs GENERATE
	targetUri AS uri,
	paragraph AS paragraph;

-- this is reduce #1
by_uri = GROUP contexts by uri;

paragraph_bag = FOREACH by_uri GENERATE
	group as uri, contexts.paragraph as paragraphs;

--TOKENIZE, REMOVE STOPWORDS AND COUNT HERE
contexts = FOREACH paragraph_bag GENERATE
	uri, tokens(paragraphs) as tokens;

freq_sorted = FOREACH contexts {
	unsorted = tokens.(token, count);
        filtered = FILTER unsorted BY (count >= $MIN_COUNT);
	-- sort descending
	sorted = ORDER filtered BY count desc;
	GENERATE
	 uri, sorted;
}

STORE freq_sorted INTO '$DIR/token_counts.JSON.bz2' USING JsonStorage(); 
