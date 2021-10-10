__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import copy
import pytest
import random

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
                            scores={'cosine': random.random()},
                        )
                        for j in range(3)
                    ],
                    chunks=[
                        Document(
                            id=f'doc {i}, chunk {j}',
                            matches=[
                                Document(
                                    id=f'doc {i}, chunk {j}, match {k}',
                                    scores={'cosine': random.random()},
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


@pytest.mark.parametrize('top_k', (3, 5))
def test_root_traversal(docs_matrix, top_k):
    executor = MatchMerger()
    document_array = executor.merge(
        docs_matrix=docs_matrix, parameters={'top_k': top_k}
    )
    assert len(document_array) == 2
    for d in document_array:
        assert len(d.matches) == top_k


@pytest.mark.parametrize('top_k', (3, 5))
def test_chunk_traversal(docs_matrix, top_k):
    executor = MatchMerger(traversal_paths=('c',))
    document_array = executor.merge(
        docs_matrix=docs_matrix, parameters={'top_k': top_k}
    )
    assert len(document_array) == 6
    for d in document_array:
        assert len(d.matches) == top_k


@pytest.mark.parametrize('top_k', (3, 5))
def test_top_k(docs_matrix, top_k):
    executor = MatchMerger()
    document_array = executor.merge(
        docs_matrix=copy.deepcopy(docs_matrix), parameters={'top_k': top_k}
    )
    expected_results = {}
    for docs in docs_matrix:
        executor._merge_shard(expected_results, docs, ('r',))
    for doc in document_array:
        expected_matches = DocumentArray(
            sorted(
                expected_results[doc.id].matches,
                key=lambda m: m.scores['cosine'].value,
                reverse=True,
            )
        )

        assert [match.scores['cosine'].value for match in doc.matches] == [
            expected_match.scores['cosine'].value
            for expected_match in expected_matches[:top_k]
        ]
