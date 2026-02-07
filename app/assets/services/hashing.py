import asyncio
import os
from typing import IO

DEFAULT_CHUNK = 8 * 1024 * 1024

_blake3 = None


def _get_blake3():
    global _blake3
    if _blake3 is None:
        try:
            from blake3 import blake3 as _b3
            _blake3 = _b3
        except ImportError:
            raise ImportError(
                "blake3 is required for asset hashing. Install with: pip install blake3"
            )
    return _blake3


def compute_blake3_hash(
    fp: str | IO[bytes],
    chunk_size: int = DEFAULT_CHUNK,
) -> str:
    if hasattr(fp, "read"):
        return _hash_file_obj(fp, chunk_size)

    with open(os.fspath(fp), "rb") as f:
        return _hash_file_obj(f, chunk_size)


async def compute_blake3_hash_async(
    fp: str | IO[bytes],
    chunk_size: int = DEFAULT_CHUNK,
) -> str:
    if hasattr(fp, "read"):
        return await asyncio.to_thread(compute_blake3_hash, fp, chunk_size)

    def _worker() -> str:
        with open(os.fspath(fp), "rb") as f:
            return _hash_file_obj(f, chunk_size)

    return await asyncio.to_thread(_worker)


def _hash_file_obj(file_obj: IO, chunk_size: int = DEFAULT_CHUNK) -> str:
    if chunk_size <= 0:
        chunk_size = DEFAULT_CHUNK

    orig_pos = file_obj.tell()

    try:
        if orig_pos != 0:
            file_obj.seek(0)

        h = _get_blake3()()
        while True:
            chunk = file_obj.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
        return h.hexdigest()
    finally:
        if orig_pos != 0:
            file_obj.seek(orig_pos)
