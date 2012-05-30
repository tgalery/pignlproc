/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 * Chris: Test script to output Ngram counts
 * @params $DIR - the directory where the files should be stored
 *         $INPUT
 *         $PIGNLPROC_JAR   
 *         $LANG 
 *         $MAX_NGRAM_LENGTH
 *         $MIN_SURFACE_FORM_LENGTH
 *
 */


SET job.name 'Output Only Parsed nonredirects for $LANG'


-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

-- Define alias for redirect resolver function
DEFINE resolve pignlproc.helpers.SecondIfNotNullElseFirst();


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

-- Wikipedia IDs
--ids = FOREACH parsedNonRedirects GENERATE
--  title,
--  id,
--  pageUrl;

--Now output to .TSV -Last directory in $dir is hard-coded
STORE parsedNonRedirects INTO '$DIR/test_parsed_nonredirects.TSV' USING PigStorage();

--TEST -- FOR DEVELOPMENT ONLY

DUMP parsedNonRedirects;
DESCRIBE parsedNonRedirects;

