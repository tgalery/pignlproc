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
DEFINE tokens pignlproc.index.Tokenizer();


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
  FLATTEN(pignlproc.evaluation.ParagraphsWithLink(text, links, paragraphs))
  AS (paragraphIdx, paragraph, targetUri, startPos, endPos);


--TOKENIZE HERE
contexts = FOREACH paragraphs GENERATE
	targetUri, FLATTEN(tokens(paragraph)) AS word;

lowercase = FOREACH contexts GENERATE
	targetUri, LOWER(word) as word;

no_punct = FILTER lowercase by not (word matches '[!\\(\\)\\-;\\\':.,"]'); 

-- testing stopword removal --> this should be a param!
stopwords = LOAD 'stopwords.en.list' USING PigStorage('\n')
	AS stopword: chararray;

with_stopwords = JOIN no_punct by word LEFT OUTER, stopwords by stopword USING 'replicated';

without_stopwords = FILTER with_stopwords BY stopwords::stopword is null;

cleaned = FOREACH without_stopwords GENERATE
	no_punct::targetUri as targetUri,
	no_punct::word as word;
		

tokens_by_uri = GROUP cleaned by (targetUri, word);

uri_token_counts = FOREACH tokens_by_uri GENERATE
	group.targetUri AS uri, group.word AS token, COUNT(cleaned.$0) AS count;

by_uri_all_tokens = GROUP uri_token_counts BY uri;

counts = FOREACH by_uri_all_tokens GENERATE
	group, uri_token_counts.(token, count); 

--Now output to .TSV --> Last directory in dir is hard-coded for now
STORE counts INTO '$DIR/token_counts.TSV' USING PigStorage();

--TEST
--DUMP counts;
--DESCRIBE counts;
