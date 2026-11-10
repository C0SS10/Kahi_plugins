"""Persistent ids for works: the first available id in WORK_ID_PRIORITY.

Every source keeps its lowest id, so the choice never depends on the order of
``external_ids``. CIARP ids are row numbers of a spreadsheet, and MAG and PMID
ids are already covered by OpenAlex, so none of them are used.
"""

WORK_ID_PRIORITY = [
    "doi",
    "openalex",
    "scienti",
    "minciencias",
    "handle",
    "dspace",
    "isbn",
    "scholar",
    "mongodb_id",
]

# A real handle is "<prefix>/<suffix>"; 123456789 is DSpace's default prefix, shared by unregistered repositories.
HANDLE_REGEX = r"^oai:[^:]+:((?!123456789/)\d[\d.]*/\d+)$"
DOI_REGEX = r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)?(10\.\d{4,9}/\S+)$"
OPENALEX_REGEX = r"(?:^|/)(W\d+)$"
ISBN_REGEX = r"^(\d{9}[\dX]|\d{13})$"


def _of_source(source, bson_type="string"):
    return {
        "$filter": {
            "input": {"$ifNull": ["$external_ids", []]},
            "cond": {
                "$and": [
                    {"$eq": ["$$this.source", source]},
                    {"$eq": [{"$type": "$$this.id"}, bson_type]},
                ]
            },
        }
    }


def _strings(source):
    return {"$map": {"input": _of_source(source), "in": {"$trim": {"input": "$$this.id"}}}}


def _capture(values, regex, options=""):
    """First group of ``regex`` in each value, null when it does not match."""
    match = {"$regexFind": {"input": "$$item", "regex": regex, "options": options}}
    return {
        "$map": {
            "input": values,
            "as": "item",
            "in": {"$let": {"vars": {"m": match}, "in": {"$arrayElemAt": ["$$m.captures", 0]}}},
        }
    }


def _lowest(values):
    """Smallest non-empty value; null when there is none."""
    return {"$min": {"$filter": {"input": values, "cond": {"$gt": ["$$this", ""]}}}}


def _doi():
    dois = _capture(_strings("doi"), DOI_REGEX, "i")
    return _lowest({"$map": {"input": dois, "in": {"$toLower": "$$this"}}})


def _scienti():
    """Scienti stores {COD_RH, COD_PRODUCTO}; the id is "COD_RH-COD_PRODUCTO"."""
    ids = {
        "$map": {
            "input": _of_source("scienti", "object"),
            "in": {"$concat": [{"$toString": "$$this.id.COD_RH"}, "-", {"$toString": "$$this.id.COD_PRODUCTO"}]},
        }
    }
    return _lowest(ids)


def _isbn():
    """Only for books: chapters and conference papers share the ISBN of the whole volume."""
    impactu_types = {
        "$map": {
            "input": {"$filter": {"input": {"$ifNull": ["$types", []]}, "cond": {"$eq": ["$$this.source", "impactu"]}}},
            "in": "$$this.type",
        }
    }
    # One isbn entry can hold several, as "9781420053432, 9781420053418" or with hyphens.
    parts = {
        "$reduce": {
            "input": _strings("isbn"),
            "initialValue": [],
            "in": {"$concatArrays": ["$$value", {"$split": ["$$this", ","]}]},
        }
    }
    isbns = {
        "$map": {
            "input": parts,
            "in": {"$toUpper": {"$replaceAll": {"input": {"$replaceAll": {"input": "$$this", "find": "-", "replacement": ""}}, "find": " ", "replacement": ""}}},
        }
    }
    valid = {"$filter": {"input": isbns, "cond": {"$regexMatch": {"input": "$$this", "regex": ISBN_REGEX}}}}
    is_book = {
        "$and": [
            {"$in": ["Libro", impactu_types]},
            {"$not": [{"$in": ["Capítulo de libro", impactu_types]}]},
        ]
    }
    return {"$cond": [is_book, _lowest(valid), None]}


def persistent_id_candidates():
    """One {source, id} per WORK_ID_PRIORITY entry, id null when the work has none of that source."""
    ids = {
        "doi": _doi(),
        "openalex": _lowest(_capture(_strings("openalex"), OPENALEX_REGEX, "i")),
        "scienti": _scienti(),
        "minciencias": _lowest(_strings("minciencias")),
        "handle": _lowest(_capture(_strings("dspace"), HANDLE_REGEX)),
        # The full OAI id when the repository has no registered handle: unique within its repository.
        "dspace": _lowest(_strings("dspace")),
        "isbn": _isbn(),
        "scholar": _lowest(_strings("scholar")),
        "mongodb_id": {"$toString": "$_id"},
    }
    return [{"source": source, "id": ids[source]} for source in WORK_ID_PRIORITY]


def _three_digits(number):
    text = {"$toString": number}
    return {"$concat": [{"$substrCP": ["000", 0, {"$max": [0, {"$subtract": [3, {"$strLenCP": text}]}]}]}, text]}


def persistent_id_assignment():
    """Each work as {_id, persistent_id: {source, id}}, without the rest of the document.

    Works that share the id (duplicates the ETL did not join, or chapters with the
    DOI of their book) get "-000", "-001"... ordered by their other ids in
    WORK_ID_PRIORITY, so the order does not depend on the ObjectId unless every
    other id is equal.
    """
    return [
        {"$project": {"_id": 1, "candidates": persistent_id_candidates()}},
        {"$set": {
            "persistent_id": {"$first": {"$filter": {"input": "$candidates", "cond": {"$gt": ["$$this.id", ""]}}}},
            # \u0001 sorts below any character of an id, so a work missing an id comes first.
            "order": {"$reduce": {
                "input": "$candidates",
                "initialValue": "",
                "in": {"$concat": ["$$value", "\u0001", {"$ifNull": ["$$this.id", ""]}]},
            }},
        }},
        {"$setWindowFields": {
            "partitionBy": "$persistent_id",
            "sortBy": {"order": 1},
            "output": {
                "position": {"$documentNumber": {}},
                "shared_by": {"$count": {}, "window": {"documents": ["unbounded", "unbounded"]}},
            },
        }},
        {"$set": {"persistent_id.id": {"$cond": [
            {"$gt": ["$shared_by", 1]},
            {"$concat": ["$persistent_id.id", "-", _three_digits({"$subtract": ["$position", 1]})]},
            "$persistent_id.id",
        ]}}},
        {"$project": {"_id": 1, "persistent_id": 1}},
    ]


def work_persistent_id_pipeline():
    """Every work complete and unchanged except for ``_id``, now its persistent id.

    The ObjectId is not kept: it changes in every release anyway. ``_id`` cannot
    change in place, so set_work_persistent_ids writes this output over works.
    The id is computed on {_id, candidates} only and the full work joined at the
    end, so the window sort never moves whole documents (works is ~40 GB).
    """
    return persistent_id_assignment() + [
        {"$lookup": {"from": "works", "localField": "_id", "foreignField": "_id", "as": "work"}},
        {"$replaceWith": {"$mergeObjects": [{"$first": "$work"}, {"_id": "$persistent_id.id"}]}},
    ]


def set_work_persistent_ids(db):
    """Replace the ObjectId ``_id`` of every work with its persistent id.

    ``$out`` into works itself fills a temporary collection created with the
    indexes of works and renames it over works when it ends: readers never see
    a half-done collection and, if it fails, works is left as it was. It needs
    free disk for a second copy of works while it runs.

    Returns the number of works per source of their id, in WORK_ID_PRIORITY order.
    """
    # The ids depend only on external_ids and types, so when no ObjectId is left a rerun would rewrite works unchanged.
    if db["works"].find_one({"_id": {"$type": "objectId"}}, {"_id": 1}):
        db["works"].aggregate(work_persistent_id_pipeline() + [{"$out": "works"}], allowDiskUse=True)

    rows = db["works"].aggregate(
        persistent_id_assignment() + [{"$group": {"_id": "$persistent_id.source", "works": {"$sum": 1}}}],
        allowDiskUse=True,
    )
    counts = {row["_id"]: row["works"] for row in rows}
    return {source: counts[source] for source in WORK_ID_PRIORITY if source in counts}
