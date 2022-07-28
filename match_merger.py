__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

from typing import List, Tuple, Optional

from jina import DocumentArray, Executor, requests
import warnings

class MatchMerger(Executor):
    """
    The MatchMerger merges the results of shards by appending all matches..
    """

    def __init__(self,
                 default_access_paths: Tuple[str, ...] = '@r',
                 default_traversal_paths: Optional[Tuple[str, ...]] = None,
                 **kwargs):
        """
        :param default_access_paths: traverse path on docs, e.g. '@r', '@c'
        :param default_traversal_paths: please use default_access_paths
        """
        super().__init__(**kwargs)
        warnings.warn('The functionality of MatchMerger is subsumed by the default behaviour starting with'
                                 'Jina 3. Consider dropping MatchMerger from your flows. MatchMerger might stop working'
                                 'with future versions of Jina or Jina Hub.', DeprecationWarning)

        if default_traversal_paths is not None:
            warnings.warn("'default_traversal_paths' will be deprecated in the future, please use 'default_access_paths'.",
                          DeprecationWarning,
                          stacklevel=2)
            self.default_access_paths = default_traversal_paths
        else:
            self.default_access_paths = default_access_paths

    @requests
    def merge(self, docs_matrix: List[DocumentArray] = [], parameters: dict = {}, **kwargs):
        access_paths = parameters.get(
            'access_paths', self.default_access_paths
        )
        results = {}
        if not docs_matrix:
            return
        for docs in docs_matrix:
            self._merge_shard(results, docs, access_paths)
        return DocumentArray(list(results.values()))

    def _merge_shard(self, results, docs, access_paths):
        for doc in docs[access_paths]:
            if doc.id in results:
                results[doc.id].matches.extend(doc.matches)
            else:
                results[doc.id] = doc
