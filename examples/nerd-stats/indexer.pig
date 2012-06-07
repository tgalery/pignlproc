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

contexts = FOREACH paragraphs GENERATE
	targetUri, paragraph;

byTargetUri = GROUP contexts BY targetUri;

contextBag = FOREACH byTargetUri GENERATE
	group, contexts.$1;


--We use a tokenization and counting UDF here
counts = FOREACH contextBag GENERATE
	group,	
	pignlproc.index.GetCounts($1);

--Now output to .TSV --> Last directory in $dir is hard-coded for now
STORE counts INTO '$DIR/token_counts.TSV' USING PigStorage();

--TEST
--DUMP counts;
--DESCRIBE counts;

