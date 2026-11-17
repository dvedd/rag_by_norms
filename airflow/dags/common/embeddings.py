from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from FlagEmbedding import BGEM3FlagModel


@lru_cache(maxsize=1)
def get_bge_m3() -> "BGEM3FlagModel":
    """Return the worker's cached BGE-M3 model using the available device."""
    import torch
    from FlagEmbedding import BGEM3FlagModel

    use_cuda = torch.cuda.is_available()

    return BGEM3FlagModel(
        model_name_or_path="BAAI/bge-m3",
        use_fp16=use_cuda,
        device="cuda" if use_cuda else "cpu",
    )
