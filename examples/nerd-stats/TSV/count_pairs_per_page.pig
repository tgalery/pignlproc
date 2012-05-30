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


SET job.name 'Wikipedia SF-URI per-page counts for $LANG'


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

-- Load Redirects and build transitive closure
-- (resolve recursively) in 2 iterations
r1a = FOREACH parsedRedirects GENERATE
  pageUrl AS source1a,
  redirect AS target1a;
r1b = FOREACH r1a GENERATE
  source1a AS source1b,
  target1a AS target1b;
r1join = JOIN
  r1a BY target1a LEFT,
  r1b BY source1b;

r2a = FOREACH r1join GENERATE
  source1a AS source2a,
  flatten(resolve(target1a, target1b)) AS target2a;
r2b = FOREACH r2a GENERATE
  source2a AS source2b,
  target2a AS target2b;
r2join = JOIN
  r2a BY target2a LEFT,
  r2b BY source2b;

redirects = FOREACH r2join GENERATE 
  source2a AS redirectSource,
  FLATTEN(resolve(target2a, target2b)) AS redirectTarget;

--FOR DEVELOPMENT ONLY: no transitive closure
--redirects = FOREACH parsedRedirects GENERATE pageUrl AS redirectSource, redirect AS redirectTarget;

-- Project articles
articles = FOREACH parsedNonRedirects GENERATE
  pageUrl,
  text,
  links,
  paragraphs;

-- Extract sentence contexts of the links respecting the paragraph boundaries
sentences = FOREACH articles GENERATE
  pageUrl,
  FLATTEN(pignlproc.evaluation.SentencesWithLink(text, links, paragraphs))
  AS (sentenceIdx, sentence, targetUri, startPos, endPos);

-- Project to three important relations
pageLinks = FOREACH sentences GENERATE
  TRIM(SUBSTRING(sentence, startPos, endPos)) AS surfaceForm,
  targetUri AS uri,
  pageUrl AS pageUrl;

-- Filter out surfaceForms that have zero or one character
pageLinksNonEmptySf = FILTER pageLinks 
  BY SIZE(surfaceForm) >= $MIN_SURFACE_FORM_LENGTH;

-- Resolve redirects
pageLinksRedirectsJoin = JOIN
  redirects BY redirectSource RIGHT,
  pageLinksNonEmptySf BY uri;
resolvedLinks = FOREACH pageLinksRedirectsJoin GENERATE
  surfaceForm,
  FLATTEN(resolve(uri, redirectTarget)) AS uri,
  pageUrl;
distinctLinks = DISTINCT resolvedLinks;

-- Make Ngrams
pageNgrams = FOREACH articles GENERATE
  FLATTEN(ngramGenerator(text)) AS ngram,
  pageUrl;

-- Double links: if surface form is annotated once,
-- it's annotated every time for one page.
-- (Need left outer join because of tokenizer)
doubledLinks = JOIN
  distinctLinks BY (pageUrl, surfaceForm) LEFT,
  pageNgrams BY (pageUrl, ngram);
pairs = FOREACH doubledLinks GENERATE
  surfaceForm,
  uri;


--------------------
-- count
--------------------

-- Count pairs: per page
distinctPairGrp = GROUP distinctLinks BY (surfaceForm, uri);
pagePairCounts = FOREACH distinctPairGrp GENERATE
  FLATTEN($0) AS (pagePairSf, pagePairUri),
  COUNT($1) AS pagePairCount;

--Now output to .TSV -Last directory in $dir is hard-coded
STORE pagePairCounts INTO '$DIR/count_pairs_per_page.TSV' USING PigStorage();

--TEST
--DUMP pagePairCounts;
--DESCRIBE pagePairCounts;
