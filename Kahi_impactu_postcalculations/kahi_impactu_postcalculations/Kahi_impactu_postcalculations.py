from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase
from pymongo import MongoClient
import subprocess
from spacy import cli, load
from kahi_impactu_postcalculations.process_one import (
    network_creation_process_one,
    top_words_process_one,
    top_words_from_works_scan,
    count_works_one,
    load_nlp_models,
)
from kahi_impactu_postcalculations.indexes import create_indexes
from kahi_impactu_postcalculations.denormalization import denormalize
from kahi_impactu_postcalculations.typing import build_type_lookup, process_type
from kahi_impactu_postcalculations.topics import (
    load_openalex_topics,
    process_topic_batch,
)
from kahi_impactu_postcalculations.person_persistent_ids import (
    process_person_id,
    recover_person_id_migration,
)
from kahi_impactu_postcalculations.work_persistent_ids import set_work_persistent_ids
from kahi_impactu_type_catalog import get_impactu_catalog
import requests
from itertools import islice


def batched(iterable, size):
    """Yield bounded lists from a cursor without materializing it completely."""
    iterator = iter(iterable)
    while batch := list(islice(iterator, size)):
        yield batch


class Kahi_impactu_postcalculations(KahiBase):
    """
    Plugin for performing post calculations for Impactu.

    This class extends KahiBase and implements functions for creating co-authorship networks and
    extracting top words
    """

    config = {}

    def __init__(self, config):
        """
        Initialize the Kahi_impactu_postcalculations plugin.
        """
        self.config = config

        self.mongodb_url = config["database_url"]
        self.database_name = config["database_name"]

        self.impactu_database_url = config["impactu_postcalculations"]["database_url"]
        self.impactu_database_name = config["impactu_postcalculations"]["database_name"]
        self.backend = self.config["impactu_postcalculations"]["backend"]
        self.verbose = self.config["impactu_postcalculations"]["verbose"]
        self.n_jobs = self.config["impactu_postcalculations"]["n_jobs"]
        self.openalex_database_url = self.config["impactu_postcalculations"]["openalex_database_url"]
        self.openalex_database_name = self.config["impactu_postcalculations"]["openalex_database_name"]
        self.inference_endpoint = self.config["impactu_postcalculations"]["inference_endpoint"]
        self.force_recalculate = self.config["impactu_postcalculations"].get(
            "force_recalculate", False
        )
        self.networks_enabled = self.config["impactu_postcalculations"].get(
            "networks_enabled", True
        )
        self.topics_enabled = self.config["impactu_postcalculations"].get(
            "topics_enabled", True
        )
        self.topic_jobs = self.config["impactu_postcalculations"].get(
            "topic_jobs", min(self.n_jobs, 4)
        )
        self.topic_batch_size = self.config["impactu_postcalculations"].get(
            "topic_batch_size", 1
        )
        if self.topic_batch_size < 1:
            raise ValueError("topic_batch_size must be greater than zero")
        self.topic_request_timeout = self.config["impactu_postcalculations"].get(
            "topic_request_timeout", 300
        )
        self.topic_request_retries = self.config["impactu_postcalculations"].get(
            "topic_request_retries", 3
        )
        if self.topic_request_retries < 1:
            raise ValueError("topic_request_retries must be greater than zero")
        self.denormalization_enabled = self.config["impactu_postcalculations"].get(
            "denormalization_enabled", True
        )
        self.top_words_backend = self.config["impactu_postcalculations"].get(
            "top_words_backend", "threading"
        )
        if self.top_words_backend not in ["threading", "multiprocessing"]:
            raise ValueError("top_words_backend must be 'threading' or 'multiprocessing'")
        self.top_words_jobs = self.config["impactu_postcalculations"].get(
            "top_words_jobs", min(self.n_jobs, 4)
        )
        if self.top_words_jobs < 1:
            raise ValueError("top_words_jobs must be greater than zero")
        self.top_words_strategy = self.config["impactu_postcalculations"].get(
            "top_words_strategy", "legacy"
        )
        if self.top_words_strategy not in ["legacy", "works_scan"]:
            raise ValueError("top_words_strategy must be 'legacy' or 'works_scan'")
        self.top_words_batch_size = self.config["impactu_postcalculations"].get(
            "top_words_batch_size", 5000
        )
        if self.top_words_batch_size < 1:
            raise ValueError("top_words_batch_size must be greater than zero")
        self.parallel_collections = True
        self.collection_jobs = 3

        self.author_count = self.config["impactu_postcalculations"][
            "author_count"] if "author_count" in self.config["impactu_postcalculations"] else 6
        self._check_and_install_spacy_models()
        self.types_priority = ["minciencias", "scienti", "ciarp",
                               "coar", "redcol", "eu-repo", "openalex", "scholar", "crossref"]
        self.person_priority = ["scienti", "orcid",
                                "scholar", "openalex", "mongodb_id"]
        self.type_catalog = get_impactu_catalog()
        self.types = self.type_catalog

    def _check_and_install_spacy_models(self):
        """
        Check if the spaCy models are installed and install them if needed.
        """
        print("INFO: Checking spaCy models")
        if not self.are_spacy_models_installed():
            print("INFO: Installing spaCy models")
            self.install_spacy_models()
        self.en_model = load('en_core_web_sm')
        self.es_model = load('es_core_news_sm')
        self.stopwords = self.en_model.Defaults.stop_words.union(
            self.es_model.Defaults.stop_words)
        # Load the spaCy models in process one for parallel computing
        load_nlp_models()

    def are_spacy_models_installed(self):
        """
        Check if the required spaCy models are installed.

        Returns:
            bool: True if models are installed, False otherwise.
        """
        return "en_core_web_sm" in cli.info()["pipelines"].keys() and "es_core_news_sm" in cli.info()["pipelines"].keys()

    def install_spacy_models(self):
        """
        Install the required spaCy models.
        """
        subprocess.run(["python3", "-m", "spacy",
                       "download", "en_core_web_sm"])
        subprocess.run(["python3", "-m", "spacy",
                       "download", "es_core_news_sm"])

    def check_topic_inference_service(self):
        """Fail fast when topic inference is enabled but unavailable."""
        ping_endpoint = self.inference_endpoint.rsplit("/", 1)[0] + "/ping"
        try:
            response = requests.get(ping_endpoint, timeout=10)
            response.raise_for_status()
        except requests.RequestException as error:
            raise RuntimeError(
                f"Topic inference service is unavailable at {ping_endpoint}"
            ) from error

    def process_types(self, db):
        warnings_enabled = self.verbose >= 4
        for source in self.types_priority:
            print(f"INFO: processing types for {source}")
            type_lookup = build_type_lookup(self.types, source)
            pipe = [{"$match": {"types.source": {"$ne": "impactu"}}},
                    {"$match": {"types.source": source}},
                    {"$unwind": "$types"},
                    {"$match": {"types.source": source}},
                    {"$group": {"_id": "$_id", "types": {"$push": "$types"}}},
                    {"$set": {
                        "types": {
                            "$sortArray": {"input": "$types", "sortBy": {"level": 1}}
                        }
                    }
            },
                {"$project": {"types": 1}},
            ]
            data = db["works"].aggregate(pipe)
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading",
            )(
                delayed(process_type)(
                    db,
                    work,
                    source,
                    type_lookup,
                    verbose=warnings_enabled,
                )
                for work in data
            )

    def process_person_ids(self, client):
        db = client[self.database_name]
        existing_collections = set(db.list_collection_names())
        product_cols = [
            db[collection_name]
            for collection_name in ("works", "patents", "events", "projects")
            if collection_name in existing_collections
        ]

        # These indexes must exist before reference migration. Creating them
        # afterwards would make every update scan the complete collection.
        for product_col in product_cols:
            product_col.create_index("authors.id")
        db["person"].create_index("external_ids.source")
        db["person"].create_index("_id_migration_complete")

        print("INFO: Recovering interrupted persistent id migrations")
        recovery_cursor = db["person"].find(
            {
                "_id_old": {"$exists": True},
                "_id_migration_complete": {"$ne": True},
            }
        )
        Parallel(n_jobs=self.n_jobs, backend="threading", verbose=10)(
            delayed(recover_person_id_migration)(
                db["person"], product_cols, person
            )
            for person in recovery_cursor
        )

        for source in self.person_priority:
            print("INFO: PERSISTENT ID SOURCE  ", source)
            # Paso 1: Buscar todos los documentos 'person' (con o sin COD_RH)
            if source == "mongodb_id":
                # Si el source es 'mongodb_id', buscar por _id
                cursor = db["person"].find(
                    {"_id_old": {"$exists": False}}
                )
            else:
                # Si el source no es 'mongodb_id', buscar por external_ids.source
                cursor = db["person"].find(
                    {"_id_old": {"$exists": False}, "external_ids.source": source})

            Parallel(n_jobs=self.n_jobs, backend="threading", verbose=10)(
                delayed(process_person_id)(client, db["person"], product_cols, person, source) for person in cursor
            )

    def process_networks(self, db, client, impactu_client):
        """Build co-authorship networks for affiliations and people."""
        print("INFO: Getting authors and affiliations ids")
        institutions_ids = []
        for aff in db["affiliations"].find(
            {"types.type": {"$nin": ["faculty", "department", "group"]}},
            {"_id": 1},
        ):
            count = db["works"].count_documents(
                {"authors.affiliations.id": aff["_id"]}
            )
            if count != 0:
                institutions_ids.append(aff["_id"])

        print("INFO: Creating affiliations networks")
        if institutions_ids:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend=self.backend,
            )(
                delayed(network_creation_process_one)(
                    self.config,
                    client if self.backend == "threading" else None,
                    impactu_client if self.backend == "threading" else None,
                    idx,
                    self.author_count,
                    "affiliations",
                    self.backend,
                    self.force_recalculate,
                )
                for idx in institutions_ids
            )

        print("INFO: Checking authors with works")
        authors_ids = [x["_id"] for x in db["person"].find({}, {"_id": 1})]
        authors_ids = Parallel(
            n_jobs=self.n_jobs,
            backend="threading",
            verbose=1,
        )(
            delayed(count_works_one)(db, author)
            for author in authors_ids
        )
        authors_ids = [x for x in authors_ids if x is not None]

        print(f"INFO: total authors {len(authors_ids)}")
        print("INFO: Creating authors networks")
        if authors_ids:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend=self.backend,
            )(
                delayed(network_creation_process_one)(
                    self.config,
                    client if self.backend == "threading" else None,
                    impactu_client if self.backend == "threading" else None,
                    idx,
                    self.author_count,
                    "person",
                    self.backend,
                    self.force_recalculate,
                )
                for idx in authors_ids
            )

    def maybe_process_networks(self, db, client, impactu_client):
        """Run network construction only when enabled by the workflow."""
        if not self.networks_enabled:
            print("INFO: Co-authorship network construction disabled")
            return False
        self.process_networks(db, client, impactu_client)
        return True

    def run(self):
        """
        Execute the plugin to create co-authorship networks and extract top words.
        """

        client = MongoClient(self.mongodb_url)
        db = client[self.database_name]

        impactu_client = MongoClient(self.impactu_database_url)
        impactu_db = impactu_client[self.impactu_database_name]

        openalex_client = MongoClient(self.openalex_database_url)
        openalex_db = openalex_client[self.openalex_database_name]

        if self.topics_enabled:
            print("INFO: Checking topic inference service")
            self.check_topic_inference_service()

        print("INFO: Setting up persistent ids for authors")
        self.process_person_ids(client)

        print("INFO: Setting up impactu types for works")
        self.process_types(db)

        # After process_types: the ISBN is only an id for works typed as books.
        print("INFO: Setting up persistent ids for works")
        for source, count in set_work_persistent_ids(db).items():
            print(f"INFO: works with persistent id from {source}: {count}")

        if self.denormalization_enabled:
            print(f"INFO: Denormalizing data in {self.database_name}")
            denormalize(
                db,
                parallel_collections=self.parallel_collections,
                max_parallel_jobs=self.collection_jobs,
            )
        else:
            print("INFO: Denormalization disabled by workflow")

        print(f"INFO: Creating indexes in db {self.database_name} for backend")
        db["works"].create_index("authors.id")
        db["patents"].create_index("authors.id")
        db["events"].create_index("authors.id")
        db["projects"].create_index("authors.id")
        create_indexes(db)

        if self.topics_enabled:
            print("INFO: Setting up topics for works")
            topic_cache = load_openalex_topics(openalex_db["topics"])
            print(f"INFO: Cached {len(topic_cache)} OpenAlex topics")
            works_cursor = db["works"].find(
                {"primary_topic": {}, "abstracts.0": {"$exists": True}},
                {
                    "titles": 1,
                    "abstracts": 1,
                    "source": 1,
                    "primary_topic": 1,
                    "topics": 1,
                },
            )
            topic_batches = batched(works_cursor, self.topic_batch_size)
            first_batch = next(topic_batches, None)
            if first_batch is not None:
                print("INFO: Warming up topic inference before parallel requests")
                process_topic_batch(
                    db["works"],
                    topic_cache,
                    first_batch,
                    self.inference_endpoint,
                    self.topic_request_timeout,
                    self.topic_request_retries,
                )
            Parallel(
                n_jobs=self.topic_jobs,
                verbose=10,
                backend="threading",
            )(
                delayed(process_topic_batch)(
                    db["works"],
                    topic_cache,
                    batch,
                    self.inference_endpoint,
                    self.topic_request_timeout,
                    self.topic_request_retries,
                )
                for batch in topic_batches
            )
        else:
            print("INFO: Topic inference disabled by workflow")

        self.maybe_process_networks(db, client, impactu_client)

        if self.top_words_strategy == "works_scan":
            print("INFO: Creating top words with works-scan strategy")
            top_words_from_works_scan(
                db,
                impactu_db,
                self.es_model,
                self.en_model,
                self.stopwords,
                self.top_words_batch_size,
                self.force_recalculate,
            )
            return 0

        print("INFO: Creating top words for institutions")
        affiliations_cursor = list(db["affiliations"].find({}, {"_id": 1}))
        Parallel(
            n_jobs=self.top_words_jobs,
            verbose=10,
            backend=self.top_words_backend,
        )(
            delayed(top_words_process_one)(
                self.config,
                client if self.top_words_backend == "threading" else None,
                impactu_client if self.top_words_backend == "threading" else None,
                aff,
                self.stopwords,
                "affiliations",
                self.top_words_backend,
                self.force_recalculate,
            )
            for aff in affiliations_cursor
        )

        print(
            "INFO: Creating top words for others affiliations such as faculty, "
            "department, group"
        )
        affiliations_cursor = list(db["affiliations"].find(
            {"types.type": {"$in": ["faculty", "department", "group"]}},
            {"_id": 1},
        ))
        Parallel(
            n_jobs=self.top_words_jobs,
            verbose=10,
            backend=self.top_words_backend,
        )(
            delayed(top_words_process_one)(
                self.config,
                client if self.top_words_backend == "threading" else None,
                impactu_client if self.top_words_backend == "threading" else None,
                aff,
                self.stopwords,
                "affiliations_others",
                self.top_words_backend,
                self.force_recalculate,
            )
            for aff in affiliations_cursor
        )

        print("INFO: Creating top words for person")
        authors_cursor = list(db["person"].find({}, {"_id": 1}))

        Parallel(
            n_jobs=self.top_words_jobs,
            verbose=10,
            backend=self.top_words_backend,
        )(
            delayed(top_words_process_one)(
                self.config,
                client if self.top_words_backend == "threading" else None,
                impactu_client if self.top_words_backend == "threading" else None,
                author,
                self.stopwords,
                "person",
                self.top_words_backend,
                self.force_recalculate,
            )
            for author in authors_cursor
        )
