import contextlib
import mimetypes
import os
from typing import Sequence


from app.assets.database.models import Asset
from app.assets.database.queries import (
    asset_exists_by_hash,
    asset_info_exists_for_asset_id,
    delete_asset_info_by_id,
    fetch_asset_info_and_asset,
    fetch_asset_info_asset_and_tags,
    get_asset_by_hash as queries_get_asset_by_hash,
    get_asset_info_by_id,
    list_asset_infos_page,
    list_cache_states_by_asset_id,
    set_asset_info_metadata,
    set_asset_info_preview,
    set_asset_info_tags,
    update_asset_info_access_time,
    update_asset_info_name,
    update_asset_info_updated_at,
)
from app.assets.helpers import select_best_live_path
from app.assets.services.path_utils import compute_filename_for_asset
from app.assets.services.schemas import (
    AssetData,
    AssetDetailResult,
    AssetSummaryData,
    DownloadResolutionResult,
    ListAssetsResult,
    UserMetadata,
    extract_asset_data,
    extract_info_data,
)
from app.database.db import create_session


def get_asset_detail(
    asset_info_id: str,
    owner_id: str = "",
) -> AssetDetailResult | None:
    with create_session() as session:
        result = fetch_asset_info_asset_and_tags(
            session,
            asset_info_id=asset_info_id,
            owner_id=owner_id,
        )
        if not result:
            return None

        info, asset, tags = result
        return AssetDetailResult(
            info=extract_info_data(info),
            asset=extract_asset_data(asset),
            tags=tags,
        )


def update_asset_metadata(
    asset_info_id: str,
    name: str | None = None,
    tags: Sequence[str] | None = None,
    user_metadata: UserMetadata = None,
    tag_origin: str = "manual",
    owner_id: str = "",
) -> AssetDetailResult:
    with create_session() as session:
        info = get_asset_info_by_id(session, asset_info_id=asset_info_id)
        if not info:
            raise ValueError(f"AssetInfo {asset_info_id} not found")
        if info.owner_id and info.owner_id != owner_id:
            raise PermissionError("not owner")

        touched = False
        if name is not None and name != info.name:
            update_asset_info_name(session, asset_info_id=asset_info_id, name=name)
            touched = True

        computed_filename = compute_filename_for_asset(session, info.asset_id)

        new_meta: dict | None = None
        if user_metadata is not None:
            new_meta = dict(user_metadata)
        elif computed_filename:
            current_meta = info.user_metadata or {}
            if current_meta.get("filename") != computed_filename:
                new_meta = dict(current_meta)

        if new_meta is not None:
            if computed_filename:
                new_meta["filename"] = computed_filename
            set_asset_info_metadata(
                session, asset_info_id=asset_info_id, user_metadata=new_meta
            )
            touched = True

        if tags is not None:
            set_asset_info_tags(
                session,
                asset_info_id=asset_info_id,
                tags=tags,
                origin=tag_origin,
            )
            touched = True

        if touched and user_metadata is None:
            update_asset_info_updated_at(session, asset_info_id=asset_info_id)

        result = fetch_asset_info_asset_and_tags(
            session,
            asset_info_id=asset_info_id,
            owner_id=owner_id,
        )
        if not result:
            raise RuntimeError("State changed during update")

        info, asset, tag_list = result
        detail = AssetDetailResult(
            info=extract_info_data(info),
            asset=extract_asset_data(asset),
            tags=tag_list,
        )
        session.commit()

        return detail


def delete_asset_reference(
    asset_info_id: str,
    owner_id: str,
    delete_content_if_orphan: bool = True,
) -> bool:
    with create_session() as session:
        info_row = get_asset_info_by_id(session, asset_info_id=asset_info_id)
        asset_id = info_row.asset_id if info_row else None

        deleted = delete_asset_info_by_id(
            session, asset_info_id=asset_info_id, owner_id=owner_id
        )
        if not deleted:
            session.commit()
            return False

        if not delete_content_if_orphan or not asset_id:
            session.commit()
            return True

        still_exists = asset_info_exists_for_asset_id(session, asset_id=asset_id)
        if still_exists:
            session.commit()
            return True

        # Orphaned asset - delete it and its files
        states = list_cache_states_by_asset_id(session, asset_id=asset_id)
        file_paths = [
            s.file_path for s in (states or []) if getattr(s, "file_path", None)
        ]

        asset_row = session.get(Asset, asset_id)
        if asset_row is not None:
            session.delete(asset_row)

        session.commit()

        # Delete files after commit
        for p in file_paths:
            with contextlib.suppress(Exception):
                if p and os.path.isfile(p):
                    os.remove(p)

    return True


def set_asset_preview(
    asset_info_id: str,
    preview_asset_id: str | None = None,
    owner_id: str = "",
) -> AssetDetailResult:
    with create_session() as session:
        info_row = get_asset_info_by_id(session, asset_info_id=asset_info_id)
        if not info_row:
            raise ValueError(f"AssetInfo {asset_info_id} not found")
        if info_row.owner_id and info_row.owner_id != owner_id:
            raise PermissionError("not owner")

        set_asset_info_preview(
            session,
            asset_info_id=asset_info_id,
            preview_asset_id=preview_asset_id,
        )

        result = fetch_asset_info_asset_and_tags(
            session, asset_info_id=asset_info_id, owner_id=owner_id
        )
        if not result:
            raise RuntimeError("State changed during preview update")

        info, asset, tags = result
        detail = AssetDetailResult(
            info=extract_info_data(info),
            asset=extract_asset_data(asset),
            tags=tags,
        )
        session.commit()

        return detail


def asset_exists(asset_hash: str) -> bool:
    with create_session() as session:
        return asset_exists_by_hash(session, asset_hash=asset_hash)


def get_asset_by_hash(asset_hash: str) -> AssetData | None:
    with create_session() as session:
        asset = queries_get_asset_by_hash(session, asset_hash=asset_hash)
        return extract_asset_data(asset)


def list_assets_page(
    owner_id: str = "",
    include_tags: Sequence[str] | None = None,
    exclude_tags: Sequence[str] | None = None,
    name_contains: str | None = None,
    metadata_filter: dict | None = None,
    limit: int = 20,
    offset: int = 0,
    sort: str = "created_at",
    order: str = "desc",
) -> ListAssetsResult:
    with create_session() as session:
        infos, tag_map, total = list_asset_infos_page(
            session,
            owner_id=owner_id,
            include_tags=include_tags,
            exclude_tags=exclude_tags,
            name_contains=name_contains,
            metadata_filter=metadata_filter,
            limit=limit,
            offset=offset,
            sort=sort,
            order=order,
        )

        items: list[AssetSummaryData] = []
        for info in infos:
            items.append(
                AssetSummaryData(
                    info=extract_info_data(info),
                    asset=extract_asset_data(info.asset),
                    tags=tag_map.get(info.id, []),
                )
            )

        return ListAssetsResult(items=items, total=total)


def resolve_asset_for_download(
    asset_info_id: str,
    owner_id: str = "",
) -> DownloadResolutionResult:
    with create_session() as session:
        pair = fetch_asset_info_and_asset(
            session, asset_info_id=asset_info_id, owner_id=owner_id
        )
        if not pair:
            raise ValueError(f"AssetInfo {asset_info_id} not found")

        info, asset = pair
        states = list_cache_states_by_asset_id(session, asset_id=asset.id)
        abs_path = select_best_live_path(states)
        if not abs_path:
            raise FileNotFoundError(
                f"No live path for AssetInfo {asset_info_id} (asset id={asset.id}, name={info.name})"
            )

        update_asset_info_access_time(session, asset_info_id=asset_info_id)
        session.commit()

        ctype = (
            asset.mime_type
            or mimetypes.guess_type(info.name or abs_path)[0]
            or "application/octet-stream"
        )
        download_name = info.name or os.path.basename(abs_path)
        return DownloadResolutionResult(
            abs_path=abs_path,
            content_type=ctype,
            download_name=download_name,
        )
