__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

from heapq import nlargest
from typing import List, Tuple

from jina import DocumentArray, Executor, requests


class MatchMerger(Executor):
    """
    The MatchMerger merges the results of shards by appending all matches..
    """

    def __init__(
        self,
        traversal_paths: Tuple[str, ...] = ('r',),
        metric: str = 'cosine',
        **kwargs
    ):
        """
        :param traversal_paths: traverse path on docs, e.g. ['r'], ['c']
        :param metric: metric to score matches.
        """
        super().__init__(**kwargs)
        self.traversal_paths = traversal_paths
        self.metric = metric

    @requests
    def merge(self, docs_matrix: List[DocumentArray], parameters: dict, **kwargs):

        metric = parameters.get('metric_name', self.metric)
        top_k = int(parameters.get('top_k', 10))
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)

        results = {}
        for docs in docs_matrix:
            self._merge_shard(results, docs, traversal_paths)
        docs = DocumentArray(list(results.values()))
        for doc in docs:
            doc.matches = nlargest(top_k, doc.matches, lambda m: m.scores[metric].value)
        return docs

    def _merge_shard(self, results, docs, traversal_paths):
        for doc in docs.traverse_flat(traversal_paths):
            if doc.id in results:
                results[doc.id].matches.extend(doc.matches)
            else:
                results[doc.id] = doc
