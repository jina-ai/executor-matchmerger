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
        default_traversal_paths: Tuple[str, ...] = ('r',),
        metric_name: str = 'cosine',
        **kwargs
    ):
        """
        :param default_traversal_paths: traverse path on docs, e.g. ['r'], ['c']
        :param metric_name: metric_name to score matches.
        """
        super().__init__(**kwargs)
        self.default_traversal_paths = default_traversal_paths
        self._metric_name = metric_name

    @requests
    def merge(self, docs_matrix: List[DocumentArray], parameters: dict, **kwargs):

        metric_name = parameters.get('metric_name', self._metric_name)
        top_k = int(parameters.get('top_k', 10))
        traversal_paths = parameters.get(
            'traversal_paths', self.default_traversal_paths
        )

        results = {}
        for docs in docs_matrix:
            self._merge_shard(results, docs, traversal_paths)
        return self._select_top_k(
            top_k, metric_name, DocumentArray(list(results.values()))
        )

    def _select_top_k(self, top_k, metric_name, docs):
        for doc in docs:
            doc.matches = nlargest(
                top_k, doc.matches, lambda m: m.scores[metric_name].value
            )
        return docs

    def _merge_shard(self, results, docs, traversal_paths):
        for doc in docs.traverse_flat(traversal_paths):
            if doc.id in results:
                results[doc.id].matches.extend(doc.matches)
            else:
                results[doc.id] = doc
