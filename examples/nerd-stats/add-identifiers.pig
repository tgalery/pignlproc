-- Load Custom-Wikipedia mapping
customWikiMapping = LOAD '$CUSTOM_WIKIPEDIA_MAPPING'
  AS (customUri:chararray, wikiId_custom:chararray);

-- Load Wikipedia-NERD-Stats
wikiNerdStats = LOAD '$WIKI_NERD_STATS'
  AS (title, surfaceForm, ofUri, uriOf, keyphraseness, wikiId_nerdStats, prominence);

-- Join
wikiNerdStatsJoin = JOIN
  customWikiMapping BY wikiId_custom RIGHT,
  wikiNerdStats BY wikiId_nerdStats;

-- Create Wikipedia-NERD-Stats with Custom-URIs
wikiNerdStatsWithCutsomUris = FOREACH wikiNerdStatsJoin GENERATE
  title,
  surfaceForm,
  ofUri,
  uriOf,
  keyphraseness,
  customUri,
  wikiId_nerdStats,
  prominence;

-- Store
STORE wikiNerdStatsWithCutsomUris INTO '$OUTPUT';

