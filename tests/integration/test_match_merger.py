__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import pytest
from jina import Document, DocumentArray, Flow, requests
from jina.executors import BaseExecutor
from match_merger import MatchMerger


class MockShard(BaseExecutor):
    @requests
    def search(self, docs: DocumentArray, **kwargs):
        for doc in docs:
            matched_doc = Document(tags={'shard_id': self.runtime_args.pea_id})
            matched_doc.scores['cosine'] = self.runtime_args.pea_id
            doc.matches.append(matched_doc)


@pytest.fixture
def docs():
    return [Document(text=f'sample text {i}') for i in range(2)]


@pytest.mark.parametrize('shards', (1, 3, 5))
def test_match_merger(docs, shards):
    with Flow().add(
        uses=MockShard, uses_after=MatchMerger, shards=shards, polling='all'
    ) as f:
        documents = f.search(docs, return_results=True)[0].docs
        assert len(documents) == 2
        for doc in documents:
            assert [d.scores['cosine'].value for d in doc.matches] == [shards - 1]
