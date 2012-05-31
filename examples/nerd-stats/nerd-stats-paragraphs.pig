/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 */

/*
TODO think about efficiency:
 - add replicated/merge/skewed(only for inner) option to JOIN operations?
 - accumulator / algebraic interfaces apply?
 - pig.cachedbag.memusage, pig.skewed.join.reduce.memusage
 - compression of intermediates (LZO)
 - Pig 0.9 supports macros. They could help make the script more readable (esp. redirect resolution)
*/

--Chris: commented out for testing
SET job.name 'Wikipedia-NERD-Stats for $LANG'

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

-- Extract paragraph contexts of the links 
paragraphs = FOREACH articles GENERATE
  pageUrl,
  FLATTEN(pignlproc.evaluation.ParagraphsWithLink(text, links, paragraphs))
  AS (paragraphIdx, paragraph, targetUri, startPos, endPos);

-- Project to three important relations
pageLinks = FOREACH paragraphs GENERATE
  TRIM(SUBSTRING(paragraph, startPos, endPos)) AS surfaceForm,
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

-- Make Ngrams -- Chris: get all Ngrams (up to $MAX_NGRAM_LENGTH) in Wikipedia
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

-- Count pairs: absolute
pairGrp = GROUP pairs BY (surfaceForm, uri);
pairCounts = FOREACH pairGrp GENERATE
  FLATTEN($0) AS (pairSf, pairUri),
  COUNT($1) AS pairCount;

-- Count pairs: per page
distinctPairGrp = GROUP distinctLinks BY (surfaceForm, uri);
pagePairCounts = FOREACH distinctPairGrp GENERATE
  FLATTEN($0) AS (pagePairSf, pagePairUri),
  COUNT($1) AS pagePairCount;

-- Count surface forms
sfGrp = GROUP pairs BY surfaceForm;
sfCounts = FOREACH sfGrp GENERATE
  $0 AS surfaceForm,
  COUNT($1) AS sfCount;

-- Count URIs
uriGrp = GROUP pairs BY uri;
uriCounts = FOREACH uriGrp GENERATE
  $0 AS uri,
  COUNT($1) AS uriCount;

-- Count Ngrams: absolute --Chris: count all Ngrams in Wikipedia
ngrams = FOREACH pageNgrams GENERATE
  ngram;
ngramGrp = GROUP ngrams BY ngram;
ngramCounts = FOREACH ngramGrp GENERATE
  $0 as ngram,
  COUNT($1) AS ngramCount;

-- Count Ngrams: per page
pageNgramsDistinct = DISTINCT pageNgrams;
pageNgramGrp = GROUP pageNgramsDistinct BY ngram;
pageNgramCounts = FOREACH pageNgramGrp GENERATE
  $0 AS ngram,
  COUNT($1) AS ngramCount;


--------------------
-- calculate results
--------------------

-- calculate P(uri|sf), a.k.a. "ofUri"
sfJoin = JOIN
  sfCounts BY surfaceForm,
  pairCounts BY pairSf;
probUriGivenSf = FOREACH sfJoin GENERATE
  surfaceForm,
  (double)pairCount/sfCount AS ofUri,
  pairUri;


-- calculate P(sf|uri), a.k.a. "uriOf"
uriJoin = JOIN
  uriCounts BY uri,
  pairCounts BY pairUri;
probSfGivenUri = FOREACH uriJoin GENERATE
  uri,
  (double)pairCount/uriCount AS uriOf,
  pairSf;


-- calculate prominence
-- = uriCounts

-- calculate keyphraseness: absolute counts with link doubles
keyphraseJoin = JOIN
  sfCounts BY surfaceForm,
  ngramCounts BY ngram;
keyphraseness = FOREACH keyphraseJoin GENERATE
  surfaceForm,
  (double)sfCount/ngramCount AS keyphrasenessScore;

-- calculate keyphraseness2: normalize by pages
-- not written at the moment!
keyphrase2Join = JOIN
  pagePairCounts BY pagePairSf,
  pageNgramCounts BY ngram;
keyphraseness2 = FOREACH keyphrase2Join GENERATE
  pagePairSf AS surfaceForm,
  (double)pagePairCount/ngramCount AS keyphraseness2Score;


--------------------
-- Output
--------------------

-- Join into one bag
joinAll1 = JOIN
  probSfGivenUri BY (uri, pairSf),
  probUriGivenSf BY (pairUri, surfaceForm);
-- some surfaceForms have more words than $MAX_NGRAM_LENGTH
-- --> need outer join for joinAll2
joinAll2 = JOIN
  joinAll1 BY surfaceForm LEFT,
  keyphraseness BY surfaceForm;
joinAll3 = JOIN
  joinAll2 BY uri,
  uriCounts BY uri;
joinAll4 = JOIN
  ids BY pageUrl,
  joinAll3 BY uri;

-- Project and establish default values
nerdStatsTable = FOREACH joinAll4 GENERATE
  title,
  joinAll2::joinAll1::probUriGivenSf::sfCounts::surfaceForm,
  ofUri,
  uriOf,
  FLATTEN(resolve('0', keyphrasenessScore)),
  id,
  uriCount;

--Chris: TEST--
DESCRIBE nerdStatsTable;
--DUMP nerdStatsTable;

-- Store
STORE nerdStatsTable INTO '$OUTPUT';

