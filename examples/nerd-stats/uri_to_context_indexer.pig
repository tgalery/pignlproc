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

SET job.name 'URI to context index for $LANG';

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

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

--Changes for indexing on small cluster
contexts = FOREACH paragraphs GENERATE
	targetUri AS uri,
	paragraph AS paragraph;

-- this is reduce #1
by_uri = GROUP contexts by uri;

paragraph_bag = FOREACH by_uri GENERATE
	group as uri, FLATTEN(contexts.paragraph) as paragraph;

ordered = order paragraph_bag by uri;

--Now output to .TSV --> Last directory in dir is hard-coded for now
STORE ordered INTO '$DIR/uri_to_context.TSV.bz2' USING PigStorage();

--TEST
--DUMP ordered;
--DESCRIBE ordered;
-- end test


