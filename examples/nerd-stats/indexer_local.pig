/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 *
 * @params $DIR - the directory where the files should be stored
 *         $INPUT - the wikipedia XML dump
 *         $PIGNLPROC_JAR - the location of the pignlproc jar
 *         $LANG - the language of the Wikidump 
 */

SET job.name 'Wikipedia-Token-Counts-per-URI for $LANG'

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Define alias for tokenizer function
DEFINE tokens pignlproc.index.GetCounts();


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

-- Modifications to facilitate local indexing
contexts = FOREACH paragraphs GENERATE
	targetUri AS uri,
	paragraph AS paragraph;

by_uri = GROUP contexts by uri;

paragraph_bag = FOREACH by_uri GENERATE
	group, contexts.paragraph as paragraph;

--now tokenize and count
tokenized = FOREACH paragraph_bag GENERATE
	group AS uri, tokens(paragraph) AS tokens;

--Now output to .TSV --> Last directory in $dir is hard-coded for now
STORE tokenized INTO '$DIR' USING PigStorage();

