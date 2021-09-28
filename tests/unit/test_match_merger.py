__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import pytest
from jina import Document, DocumentArray
from match_merger import MatchMerger


@pytest.fixture
def docs_matrix():
    return [
        DocumentArray(
            [
                Document(
                    id=f'doc {i}',
                    matches=[
                        Document(
                            id=f'doc {i}, match {j}',
                            scores={'cosine': shard + i + j + 1},
                        )
                        for j in range(3)
                    ],
                    chunks=[
                        Document(
                            id=f'doc {i}, chunk {j}',
                            matches=[
                                Document(
                                    id=f'doc {i}, chunk {j}, match {k}',
                                    scores={'cosine': shard + i + j + k + 1},
                                )
                                for k in range(2)
                            ],
                        )
                        for j in range(3)
                    ],
                )
                for i in range(2)
            ]
        )
        for shard in range(4)
    ]


def test_root_traversal(docs_matrix):
    executor = MatchMerger()
    document_array = executor.merge(docs_matrix=docs_matrix, parameters={})
    assert len(document_array) == 2
    for idx, d in enumerate(document_array):
        assert d.matches == docs_matrix[-1][idx].matches


def test_chunk_traversal(docs_matrix):
    executor = MatchMerger(default_traversal_paths=('c',))
    document_array = executor.merge(docs_matrix=docs_matrix, parameters={})
    assert len(document_array) == 6
    for idx, d in enumerate(document_array):
        doc_idx = int(idx / 3)
        chunk_idx = idx - (doc_idx * 3)
        assert d.matches == docs_matrix[-1][doc_idx].chunks[chunk_idx].matches
