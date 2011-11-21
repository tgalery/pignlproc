-- Load Freebase data
freebaseDump = LOAD '$FREEBASE_DUMP'
  AS (mid:chararray, relation:chararray, key:chararray, value:chararray);
freebaseWikiLinks = FILTER freebaseDump BY (key == CONCAT('/wikipedia/$LANG', '_id'));
fbWikiMapping = FOREACH freebaseWikiLinks GENERATE
  value AS wikiId_fbDump,
  mid;

-- Load Wikipedia-NERD-Stats
wikiNerdStats = LOAD '$WIKI_NERD_STATS'
  AS (title, surfaceForm, ofUri, uriOf, keyphraseness, wikiId_nerdStats, prominence);

-- Join
wikiNerdStatsJoin = JOIN
  fbWikiMapping BY wikiId_fbDump,
  wikiNerdStats BY wikiId_nerdStats;

-- Create Wikipedia-NERD-Stats with Freebase MIDs
wikiNerdStatsWithMids = FOREACH wikiNerdStatsJoin GENERATE
  title,
  surfaceForm,
  ofUri,
  uriOf,
  keyphraseness,
  mid,
  wikiId_nerdStats,
  prominence;

STORE wikiNerdStatsWithMids INTO '$OUTPUT';

