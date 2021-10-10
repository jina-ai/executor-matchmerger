# MatchMerger

**MatchMerger** selects `top-k` matches out of all the matches received from each shard.

The `MatchMerger` is used in the `uses_after` attribute when adding an `Executor` to the `Flow`.

## Usage

```python
from jina import Flow, Document

f = Flow().add(
    uses='jinahub+docker://MyExecutor', 
    shards=10,
    uses_after='jinahub+docker://MatchMerger'
)
```
