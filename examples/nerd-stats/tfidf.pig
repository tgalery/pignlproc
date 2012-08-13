/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 *	- use Pig/hadoop to generate a tfidf index for the wikipedia corpus
 * @params $OUTPUT_DIR - the directory where the files should be stored
 *         $STOPLIST_PATH - the location of the stoplist in HDFS                
 *         $STOPLIST_NAME - the filename of the stoplist
 *         $INPUT - the wikipedia XML dump
 *         $MIN_COUNT - the minumum count for a token to be included in the index
 *         $PIGNLPROC_JAR - the location of the pignlproc jar
 *         $LANG - the language of the Wikidump 
 *         $MAX_SPAN_LENGTH - the maximum length for a paragraph span
 *         $NUM_DOCS - the number of documents in this wikipedia dump
 *         $N - the number of tokens to keep
 *         - use the file 'indexer.pig.params' to supply a default configuration
 */


-- TEST: set parallelism level for reducers
SET default_parallel 15;

SET job.name 'Wikipedia-Token-Counts-per-URI for $LANG';
--SET mapred.compress.map.output 'true';
--SET mapred.map.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';
-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Define aliases
DEFINE getTokens pignlproc.index.LuceneTokenizer('$STOPLIST_PATH', '$STOPLIST_NAME');
DEFINE textWithLink pignlproc.evaluation.ParagraphsWithLink('$MAX_SPAN_LENGTH');
DEFINE JsonCompressedStorage pignlproc.storage.JsonCompressedStorage();
DEFINE keepTopN pignlproc.helpers.FirstNtuples('$N');
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

--DUMP contexts;
-----------------
-- BEGIN TFIDF --
-----------------
-- (1) group by uri
--doc_token = FOREACH paragraphs GENERATE
--        targetUri AS uri,
--        FLATTEN(getTokens(paragraph)) AS token;

doc_context = FOREACH paragraphs GENERATE
        targetUri AS uri,
        getTokens(paragraph) AS context;

--add relation here to filter by number of contexts
all_contexts = GROUP doc_context by uri;

size_filter = FILTER all_contexts BY
		COUNT(doc_context) > 20;

--DESCRIBE doc_context;
--DESCRIBE all_contexts;
--DESCRIBE size_filter;
flattened_context = FOREACH size_filter {
	contexts = doc_context.context;
 	GENERATE
	group AS uri,
	FLATTEN(contexts) AS context; 
--	contexts = FLATTEN(doc_context);
--	tokenbag = contexts.context;
--	tokens = FLATTEN(tokenbag);
--	GENERATE
--	group AS uri,
--	FLATTEN(tokens) AS token;
};

uri_and_token = FOREACH flattened_context GENERATE
	uri,
	FLATTEN(context) AS token;

--DESCRIBE flattened_context;	
--DESCRIBE uri_and_token;		
	
--DESCRIBE doc_token;
--
--TODO - consider tf normalization - i.e. tf/|tokens|
--
-- (2) group by token and Unique Doc to get global doc frequency
unique = DISTINCT uri_and_token;
--
docs_by_tokens = group unique by token;
--
--DUMP docs_by_tokens;
--DESCRIBE docs_by_tokens; 
--
raw_doc_freq = FOREACH docs_by_tokens GENERATE
	group AS token,
	COUNT(unique) as df;

doc_freq = FILTER raw_doc_freq BY df > $MIN_COUNT;
--
--NUM_DOCS should be the total number of RESOURCES
idf = foreach doc_freq GENERATE
	token,
	LOG((double)$NUM_DOCS/(double)df) AS idf: double;
--DUMP idf;
--DESCRIBE idf;
--ordered = order doc_freq BY df desc;
--STORE ordered into '$OUTPUT_DIR/doc-frquency.json.bz2' USING JsonCompressedStorage;
--
--(3) Term Frequency
term_freq = GROUP uri_and_token by (uri, token);
term_counts = FOREACH term_freq GENERATE
	group.uri as uri,
	group.token as token,
	COUNT(uri_and_token) as tf;
--
--
--(4) put the data together
token_instances = JOIN term_counts BY token, idf by token; 
--DUMP token_instances;
--DESCRIBE token_instances;
--
----(5) calculate tfidf using $NUM_DOCS - note that the user must know how many RESOURCES there are, not how many docs
--DESCRIBE token_instances;
tfidf = FOREACH token_instances {
	tf_idf = (double)term_counts::tf*(double)idf::idf;
		GENERATE 
			term_counts::uri as uri,
			term_counts::token as token,
			tf_idf as weight;
	};
--DESCRIBE tfidf;
by_docs = group tfidf BY uri;
--DESCRIBE by_docs;
--DUMP by_docs;
--
docs_with_weights = FOREACH by_docs GENERATE
	group as uri,
	tfidf.(token,weight) as tokens; 
--
--DUMP docs_with_weights;
--DESCRIBE docs_with_weights; 
ordered = FOREACH docs_with_weights {
	terms = tokens;
	sorted = ORDER tokens by weight desc;
	GENERATE 
	uri, sorted;	
};

top100 = FOREACH ordered GENERATE
	uri,
	keepTopN(sorted) AS sorted;

STORE top100 INTO '$OUTPUT_DIR/token_counts.json.bz2' USING JsonCompressedStorage();
--DUMP ordered;
--DESCRIBE ordered;


----tfidf_all = FOREACH token_usages {
----              idf    = LOG((double)$NDOCS/(double)num_docs_with_token);
----              tf_idf = (double)term_freq*idf;
----                GENERATE
----                  doc_id AS doc_id,
----                  token  AS token,
----                  tf_idf AS tf_idf
----                ;
----             };
---- GENERATE
----	FLATTEN(term_counts) AS instance;
--	
--
--
--
----TEST
----ordered = ORDER counts by tf desc;
----DUMP ordered;
----DESCRIBE ordered;
--
--
--
----DESCRIBE uri_to_tokens;
----token_bag = FOREACH paragraph_bag GENERATE
----	uri, getTokens();
--
----The following steps are done to get doc freq
----grouped = GROUP uri_to_tokens ALL;
----DESCRIBE grouped;
--
----tokens = FOREACH grouped GENERATE
----	FLATTEN(uri_to_tokens.context) as tokens;
----DESCRIBE tokens;
----DUMP tokens;
