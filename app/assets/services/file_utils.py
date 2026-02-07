import os


def get_mtime_ns(stat_result: os.stat_result) -> int:
    """Extract mtime in nanoseconds from a stat result."""
    return getattr(
        stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000)
    )


def get_size_and_mtime_ns(path: str, follow_symlinks: bool = True) -> tuple[int, int]:
    """Get file size in bytes and mtime in nanoseconds."""
    st = os.stat(path, follow_symlinks=follow_symlinks)
    return st.st_size, get_mtime_ns(st)


def verify_file_unchanged(
    mtime_db: int | None,
    size_db: int | None,
    stat_result: os.stat_result,
) -> bool:
    """Check if a file is unchanged based on mtime and size.

    Returns True if the file's mtime and size match the database values.
    Returns False if mtime_db is None or values don't match.

    size_db=None means don't check size; 0 is a valid recorded size.
    """
    if mtime_db is None:
        return False
    actual_mtime_ns = get_mtime_ns(stat_result)
    if int(mtime_db) != int(actual_mtime_ns):
        return False
    if size_db is not None:
        return int(stat_result.st_size) == int(size_db)
    return True


def list_files_recursively(base_dir: str) -> list[str]:
    """Recursively list all files in a directory."""
    out: list[str] = []
    base_abs = os.path.abspath(base_dir)
    if not os.path.isdir(base_abs):
        return out
    for dirpath, _subdirs, filenames in os.walk(
        base_abs, topdown=True, followlinks=False
    ):
        for name in filenames:
            out.append(os.path.abspath(os.path.join(dirpath, name)))
    return out
