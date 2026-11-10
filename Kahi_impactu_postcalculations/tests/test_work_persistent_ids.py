import os

import pytest
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from kahi_impactu_postcalculations.work_persistent_ids import persistent_id_assignment, set_work_persistent_ids

WORK_ID = ObjectId("6ab557465418a51e3260366f")


def scienti(cod_rh, cod_producto):
    return {"source": "scienti", "id": {"COD_RH": cod_rh, "COD_PRODUCTO": cod_producto}}


def ext(source, value):
    return {"source": source, "id": value}


def impactu(*types):
    return [{"source": "impactu", "type": t} for t in types]


# Ids of 6ab557465418a51e3260366f, the example of the issue.
SCIENTI_AND_MINCIENCIAS = [
    ext("minciencias", "ART-0000576751-436"),
    scienti("0000576751", "436"),
    ext("minciencias", "ART-0000141429-480"),
    scienti("0000141429", "480"),
    ext("minciencias", "ART-0001348573-144"),
    scienti("0001348573", "144"),
]

CASES = [
    ("doi first, lowest and lowercase", [
        ext("openalex", "https://openalex.org/W1"),
        ext("doi", "https://doi.org/10.5555/B"),
        ext("doi", "http://dx.doi.org/10.1016/S0140-6736(20)30925-9"),
        *SCIENTI_AND_MINCIENCIAS,
    ], [], ("doi", "10.1016/s0140-6736(20)30925-9")),
    ("lowest openalex", [
        ext("openalex", "https://openalex.org/W3092849554"),
        ext("openalex", "https://openalex.org/W2"),
        ext("mag", "3092849554"),
    ], [], ("openalex", "W2")),
    ("lowest scienti", SCIENTI_AND_MINCIENCIAS, [], ("scienti", "0000141429-480")),
    ("lowest minciencias without scienti", [e for e in SCIENTI_AND_MINCIENCIAS if e["source"] == "minciencias"], [],
     ("minciencias", "ART-0000141429-480")),
    ("empty id is skipped", [ext("minciencias", ""), ext("minciencias", "ART-1-1")], [], ("minciencias", "ART-1-1")),
    ("registered handle over default prefix", [
        ext("dspace", "oai:repo.b.edu.co:123456789/1"),
        ext("dspace", "oai:repo.a.edu.co:20.500.12495/5478"),
    ], [], ("handle", "20.500.12495/5478")),
    ("oai id when there is no real handle", [
        ext("dspace", "oai:bibliotecadigital.univalle.edu.co:item/486"),
        ext("dspace", "oai:repo.b.edu.co:123456789/1"),
    ], [], ("dspace", "oai:bibliotecadigital.univalle.edu.co:item/486")),
    ("isbn of a book", [ext("isbn", "978-1-4200-5343-2, 9781420053418"), ext("isbn", "0"), ext("scholar", "abc")],
     impactu("Libro"), ("isbn", "9781420053418")),
    ("no isbn for a chapter", [ext("isbn", "9781420053418"), ext("scholar", "zKy1"), ext("scholar", "aB2c")],
     impactu("Libro", "Capítulo de libro"), ("scholar", "aB2c")),
    ("ciarp, mag and pmid are not used", [
        ext("ciarp", "1-2"), ext("ciarp_record", "3"), ext("mag", "4"), ext("pmid", "5"), ext("uri", "http://x"),
    ], [], ("mongodb_id", str(WORK_ID))),
]


@pytest.fixture(scope="module")
def db():
    client = MongoClient(os.environ.get("KAHI_TEST_MONGO_URL", "localhost:27017"), serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        pytest.skip("needs MongoDB >= 5.1: set KAHI_TEST_MONGO_URL")
    yield client[os.environ.get("KAHI_TEST_DATABASE", "kahi")]
    client.close()


@pytest.mark.parametrize("name, external_ids, types, expected", CASES, ids=[case[0] for case in CASES])
def test_persistent_id(db, name, external_ids, types, expected):
    # $documents runs the pipeline on these documents only: nothing is read from or written to a collection.
    document = {"_id": WORK_ID, "external_ids": external_ids, "types": types, "titles": [{"title": "kept"}]}
    [result] = db.aggregate([{"$documents": [document]}, *persistent_id_assignment()])

    assert result == {"_id": WORK_ID, "persistent_id": {"source": expected[0], "id": expected[1]}}


def test_works_sharing_an_id_get_a_suffix_ordered_by_their_other_ids(db):
    doi = ext("doi", "https://doi.org/10.7765/9781526147295")
    works = [
        {"_id": ObjectId(), "external_ids": [doi, ext("openalex", "https://openalex.org/W9")]},
        {"_id": ObjectId(), "external_ids": [doi]},
        {"_id": ObjectId(), "external_ids": [doi, ext("openalex", "https://openalex.org/W1")]},
        {"_id": ObjectId(), "external_ids": [ext("doi", "https://doi.org/10.5555/unique")]},
    ]
    results = {doc["_id"]: doc["persistent_id"]["id"] for doc in db.aggregate([{"$documents": works}, *persistent_id_assignment()])}

    assert [results[work["_id"]] for work in works] == [
        "10.7765/9781526147295-002",
        "10.7765/9781526147295-000",
        "10.7765/9781526147295-001",
        "10.5555/unique",
    ]


def test_set_work_persistent_ids_replaces_the_object_ids_of_works(db):
    # A database of its own, dropped at the end: the test never writes to kahi.
    scratch = db.client["kahi_test_work_persistent_ids"]
    db.client.drop_database(scratch.name)
    without_ids = ObjectId()
    scratch["works"].insert_many([
        {"_id": ObjectId(), "external_ids": [ext("doi", "https://doi.org/10.5555/A")], "titles": [{"title": "a"}]},
        {"_id": without_ids, "external_ids": [ext("ciarp", "1-2")], "titles": [{"title": "b"}]},
    ])
    scratch["works"].create_index("titles.title")
    try:
        for _ in range(2):  # the second run finds no ObjectId and keeps the same ids
            assert set_work_persistent_ids(scratch) == {"doi": 1, "mongodb_id": 1}
            assert list(scratch["works"].find({}, {"titles": 1}).sort("titles.title", 1)) == [
                {"_id": "10.5555/a", "titles": [{"title": "a"}]},
                {"_id": str(without_ids), "titles": [{"title": "b"}]},
            ]
            assert "titles.title_1" in scratch["works"].index_information()
    finally:
        db.client.drop_database(scratch.name)
