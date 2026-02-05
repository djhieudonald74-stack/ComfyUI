"""Helper functions for assets integration tests."""
import requests


def trigger_sync_seed_assets(session: requests.Session, base_url: str) -> None:
    """Force a synchronous sync/seed pass by calling the seed endpoint with wait=true."""
    session.post(
        base_url + "/api/assets/seed?wait=true",
        json={"roots": ["models", "input", "output"]},
        timeout=60,
    )


def get_asset_filename(asset_hash: str, extension: str) -> str:
    return asset_hash.removeprefix("blake3:") + extension
