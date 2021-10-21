# MatchMerger

**MatchMerger** merges the matches in the same request based on the `.id` attribute. The matches of the Documents with the same `id` will be merged into one Document.


## Usage
### Combine the matches from different shards
Assuming you have 20 shards and use `top-k=10`, you will get 200 results in the merger.
When adding an `Executor` to the `Flow`, `MatchMerger` is used in the `uses_after` attribute so that the executor returns one Document with 200 matches instead of 10 Documents.

### Combine the matches from different paths
`MatchMerger` is also used when you want to combine the matches retrieved from different pathways. Assuming you have two paralell retrieval pathways, one pathway retrieves 10 matches based on images and the other retrieves 10 matches based on audios. When combining the requests from the two pathways, you will receive two Documents with the same id but matches from different pathways. You can use `MatchMerger` to merge the matches into one Document with 20 matches.
