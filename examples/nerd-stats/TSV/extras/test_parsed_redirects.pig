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

/*
TODO think about efficiency:
 - add replicated/merge/skewed(only for inner) option to JOIN operations?
 - accumulator / algebraic interfaces apply?
 - pig.cachedbag.memusage, pig.skewed.join.reduce.memusage
 - compression of intermediates (LZO)
 - in pignlproc.evaluation.SentencesWithLink
   - we don't need the paragraphs
   - we don't need the sentences and the sentence indexes
 - Pig 0.9 supports macros. They could help make the script more readable (esp. redirect resolution)
*/

--Chris: commented out for testing
--SET job.name 'Output Only Parsed redirects for $LANG'


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
STORE parsedRedirects INTO '$DIR/test_parsed_redirects.TSV' USING PigStorage();

--TEST -- FOR DEVELOPMENT ONLY

DUMP parsedRedirects;
DESCRIBE parsedRedirects;

