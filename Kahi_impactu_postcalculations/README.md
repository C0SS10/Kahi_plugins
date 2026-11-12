<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi impactu postcalculations
This plugin helps to calculate netwoks, top words and some other stuff from the impactu output.

# Description
Supports the calculation of the following metrics:
- Co-authorship network 
  - affiliations
  - authors 
- Top words for 
  - affiliations 
  - authors

# Installation

## Dependencies
This package requires MongoDB to be installed and running and kahi already executed.
ImpactU type mappings are loaded from the versioned
`Kahi_impactu_type_catalog` package; this plugin no longer maintains a separate
Excel copy.

## Package
To install the package, run the following command:
`pip install kahi_impactu_postcalculations
`


# Usage

example for workflow:

```
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  impactu_postcalculations:
    database_url: localhost:27017
    database_name: kahi_calculations
    openalex_database_url: localhost:27017
    openalex_database_name: openalexco
    inference_endpoint: http://localhost:8082/invocations
    topics_enabled: true
    topic_jobs: 4
    topic_batch_size: 1
    topic_request_timeout: 300
    topic_request_retries: 3
    denormalization_enabled: true
    networks_enabled: true
    top_words_strategy: legacy
    top_words_batch_size: 500
    top_words_backend: threading
    top_words_jobs: 4
    force_recalculate: false
    backend: multiprocessing
    n_jobs: 6
    verbose: 5
    author_count: 6 #use this with warning, maybe the network is too big and it can not be saved in MongoDB
```

Notes:
- Denormalization runs with collection-level parallelization enabled by default.
- The internal denormalization parallel setup is fixed to `parallel_collections = true`.
- The internal denormalization parallel setup is fixed to `collection_jobs = 3`.
- These two values are defined in code and are not configurable from the workflow YAML.
- Topic inference sends `topic_batch_size` works per request and writes each
  completed batch with a single unordered bulk operation.
- Set `denormalization_enabled: false` only when denormalization already
  completed and the plugin is being resumed at topic inference.
- Set `force_recalculate` to `true` when the calculations database contains
  results from an older ETL and networks/top words must be refreshed.
- Set `networks_enabled` to `false` to skip affiliation and person
  co-authorship networks without disabling the remaining post-calculations.
- Set `topics_enabled` to `false` to skip topic inference when all pending
  works were already processed and the plugin is being resumed.
- `top_words_backend` and `top_words_jobs` control only the top-words phase.
  Use `threading` with a small worker count to avoid exhausting MongoDB
  connections while the general plugin backend remains unchanged.
- Set `top_words_strategy: works_scan` to compute person and affiliation
  top words by scanning `works` once. The output schema is unchanged, but
  titles are processed once per work instead of once per person/affiliation.
- Type-mapping warnings are emitted only with `verbose: 4` or higher. Type
  mappings are compiled once per source and reused by all workers.
- After the types, every work gets a persistent `_id`: its DOI, OpenAlex,
  ScienTI, Minciencias, handle, DSpace OAI, ISBN (books only) or Google
  Scholar id, in that order, or its ObjectId as text when it has none.
  The DOI drops URLs glued to it, `//` and a final `/`, which the frontend
  cannot route; a DOI with nothing left after that is skipped. Works sharing an id get `-000`, `-001`... `works` is rewritten with `$out`,
  which needs free disk for a second copy of `works` while it runs. The step
  is skipped when no work has an ObjectId `_id`.
- Type rules come from the shared catalog JSON bundled by
  `Kahi_impactu_type_catalog==0.1.0`.


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
