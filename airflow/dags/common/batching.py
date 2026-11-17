def batch_list[Item](items: list[Item], batch_size: int = 16) -> list[list[Item]]:
    """Split a list into fixed-size batches without reordering items."""
    return [
        items[index : index + batch_size] for index in range(0, len(items), batch_size)
    ]
