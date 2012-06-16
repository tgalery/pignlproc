/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 *
 * @params $DIR - the directory where the files should be stored
 *         $INPUT - the wikipedia XML dump
 *         $PIGNLPROC_JAR - the location of the pignlproc jar
 *         $LANG - the language of the Wikidump 
 */


-- TEST: set parallelism level for reducers
SET default_parallel 15;

SET job.name 'Wikipedia-Token-Counts-per-URI for $LANG';

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Define alias for tokenizer function
DEFINE tokens pignlproc.index.Lucene_Tokenizer();


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


--STORE articles INTO '$DIR/intermediate_output/articles.TSV.bz2' USING PigStorage();

-- Extract paragraph contexts of the links 
paragraphs = FOREACH articles GENERATE
  pageUrl,
  FLATTEN(pignlproc.evaluation.ParagraphsWithLink(text, links, paragraphs))
  AS (paragraphIdx, paragraph, targetUri, startPos, endPos);

-- Optimisations for distributed processing

--TOKENIZE HERE
contexts = FOREACH paragraphs GENERATE
	targetUri, FLATTEN(tokens(paragraph)) AS word;

--TEST
--DUMP contexts;
--DESCRIBE contexts;
		

tokens_by_uri = GROUP contexts by (targetUri, word);

uri_token_counts = FOREACH tokens_by_uri GENERATE
	group.targetUri AS uri, group.word AS token, COUNT(contexts.$0) AS count;

by_uri_all_tokens = GROUP uri_token_counts BY uri;

counts = FOREACH by_uri_all_tokens GENERATE
	group, uri_token_counts.(token, count); 

--Now output to .TSV --> Last directory in dir is hard-coded for now
STORE counts INTO '$DIR/token_counts.TSV.bz2' USING PigStorage();

--TEST
--DUMP counts;
--DESCRIBE tokens_by_uri;
--DESCRIBE uri_token_counts;
--DESCRIBE by_uri_all_tokens;
--DESCRIBE counts;
