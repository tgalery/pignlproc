/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 *
 * Chris: Test script to output Ngram counts
 * @params $DIR - the directory where the files should be stored
 *         $INPUT
 *         $PIGNLPROC_JAR   
 *         $LANG 
 *         $MAX_NGRAM_LENGTH
 */

SET job.name 'Wikipedia Absolute Ngram Counts for $LANG'


-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

-- Define alias for redirect resolver function
DEFINE resolve pignlproc.helpers.SecondIfNotNullElseFirst();

-- Define Ngram generator with maximum Ngram length
DEFINE ngramGenerator pignlproc.helpers.NGramGenerator('$MAX_NGRAM_LENGTH');

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
ids = FOREACH parsedNonRedirects GENERATE
  title,
  id,
  pageUrl;

-- Project articles
articles = FOREACH parsedNonRedirects GENERATE
  pageUrl,
  text,
  links,
  paragraphs;

-- Make Ngrams
pageNgrams = FOREACH articles GENERATE
  FLATTEN(ngramGenerator(text)) AS ngram,
  pageUrl;


--------------------
-- count
--------------------
-- Count Ngrams: absolute
ngrams = FOREACH pageNgrams GENERATE
  ngram;
ngramGrp = GROUP ngrams BY ngram;
ngramCounts = FOREACH ngramGrp GENERATE
  $0 as ngram,
  COUNT($1) AS ngramCount;



--Now output to .TSV
STORE ngramCounts INTO '$DIR/ngramcounts_absolute.TSV' USING PigStorage();

--TEST

--DUMP ngramCounts;
--DESCRIBE ngramCounts;



