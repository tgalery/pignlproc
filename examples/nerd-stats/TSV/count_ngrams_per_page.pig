/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 * Chris: Test script to output per-page Ngram counts
 * @params $DIR - the directory where the files should be stored
 *         $INPUT
 *         $PIGNLPROC_JAR   
 *         $LANG 
 *         $MAX_NGRAM_LENGTH
 */ 




SET job.name 'Wikipedia Count per-page Ngrams for $LANG'

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR


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

-- Count Ngrams: per page
pageNgramsDistinct = DISTINCT pageNgrams;
pageNgramGrp = GROUP pageNgramsDistinct BY ngram;
pageNgramCounts = FOREACH pageNgramGrp GENERATE
  $0 AS ngram,
  COUNT($1) AS ngramCount;

--Now output to .TSV -Last directory in $dir is hard-coded
STORE pageNgramCounts INTO '$DIR/ngramcounts_per_page.TSV' USING PigStorage();

--TEST

--DUMP pageNgramCounts;
--DESCRIBE pageNgramCounts;

