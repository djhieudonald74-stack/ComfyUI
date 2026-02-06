import logging
import os
import urllib.parse
import uuid
from typing import Any

from aiohttp import web
from pydantic import ValidationError

import folder_paths
from app import user_manager
from app.assets.api import schemas_in, schemas_out
from app.assets.api.schemas_in import (
    AssetValidationError,
    UploadError,
)
from app.assets.api.upload import parse_multipart_upload
from app.assets.seeder import asset_seeder
from app.assets.services import (
    DependencyMissingError,
    HashMismatchError,
    apply_tags,
    asset_exists,
    create_from_hash,
    delete_asset_reference,
    get_asset_detail,
    list_assets_page,
    list_tags,
    remove_tags,
    resolve_asset_for_download,
    update_asset_metadata,
    upload_from_temp_path,
)

ROUTES = web.RouteTableDef()
USER_MANAGER: user_manager.UserManager | None = None

# UUID regex (canonical hyphenated form, case-insensitive)
UUID_RE = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"


def get_query_dict(request: web.Request) -> dict[str, Any]:
    """
    Gets a dictionary of query parameters from the request.

    'request.query' is a MultiMapping[str], needs to be converted to a dictionary to be validated by Pydantic.
    """
    query_dict = {
        key: request.query.getall(key)
        if len(request.query.getall(key)) > 1
        else request.query.get(key)
        for key in request.query.keys()
    }
    return query_dict


# Note to any custom node developers reading this code:
# The assets system is not yet fully implemented, do not rely on the code in /app/assets remaining the same.


def register_assets_system(
    app: web.Application, user_manager_instance: user_manager.UserManager
) -> None:
    global USER_MANAGER
    USER_MANAGER = user_manager_instance
    app.add_routes(ROUTES)


def _build_error_response(
    status: int, code: str, message: str, details: dict | None = None
) -> web.Response:
    return web.json_response(
        {"error": {"code": code, "message": message, "details": details or {}}},
        status=status,
    )


def _build_validation_error_response(code: str, ve: ValidationError) -> web.Response:
    return _build_error_response(400, code, "Validation failed.", {"errors": ve.json()})


def _validate_sort_field(requested: str | None) -> str:
    if not requested:
        return "created_at"
    v = requested.lower()
    if v in {"name", "created_at", "updated_at", "size", "last_access_time"}:
        return v
    return "created_at"


@ROUTES.head("/api/assets/hash/{hash}")
async def head_asset_by_hash(request: web.Request) -> web.Response:
    hash_str = request.match_info.get("hash", "").strip().lower()
    if not hash_str or ":" not in hash_str:
        return _build_error_response(
            400, "INVALID_HASH", "hash must be like 'blake3:<hex>'"
        )
    algo, digest = hash_str.split(":", 1)
    if (
        algo != "blake3"
        or not digest
        or any(c for c in digest if c not in "0123456789abcdef")
    ):
        return _build_error_response(
            400, "INVALID_HASH", "hash must be like 'blake3:<hex>'"
        )
    exists = asset_exists(hash_str)
    return web.Response(status=200 if exists else 404)


@ROUTES.get("/api/assets")
async def list_assets_route(request: web.Request) -> web.Response:
    """
    GET request to list assets.
    """
    query_dict = get_query_dict(request)
    try:
        q = schemas_in.ListAssetsQuery.model_validate(query_dict)
    except ValidationError as ve:
        return _build_validation_error_response("INVALID_QUERY", ve)

    sort = _validate_sort_field(q.sort)
    order = (
        "desc"
        if (q.order or "desc").lower() not in {"asc", "desc"}
        else q.order.lower()
    )

    result = list_assets_page(
        owner_id=USER_MANAGER.get_request_user_id(request),
        include_tags=q.include_tags,
        exclude_tags=q.exclude_tags,
        name_contains=q.name_contains,
        metadata_filter=q.metadata_filter,
        limit=q.limit,
        offset=q.offset,
        sort=sort,
        order=order,
    )

    summaries = [
        schemas_out.AssetSummary(
            id=item.info.id,
            name=item.info.name,
            asset_hash=item.asset.hash if item.asset else None,
            size=int(item.asset.size_bytes)
            if item.asset and item.asset.size_bytes
            else None,
            mime_type=item.asset.mime_type if item.asset else None,
            tags=item.tags,
            created_at=item.info.created_at,
            updated_at=item.info.updated_at,
            last_access_time=item.info.last_access_time,
        )
        for item in result.items
    ]

    payload = schemas_out.AssetsList(
        assets=summaries,
        total=result.total,
        has_more=(q.offset + len(summaries)) < result.total,
    )
    return web.json_response(payload.model_dump(mode="json", exclude_none=True))


@ROUTES.get(f"/api/assets/{{id:{UUID_RE}}}")
async def get_asset_route(request: web.Request) -> web.Response:
    """
    GET request to get an asset's info as JSON.
    """
    asset_info_id = str(uuid.UUID(request.match_info["id"]))
    try:
        result = get_asset_detail(
            asset_info_id=asset_info_id,
            owner_id=USER_MANAGER.get_request_user_id(request),
        )
        if not result:
            return _build_error_response(
                404,
                "ASSET_NOT_FOUND",
                f"AssetInfo {asset_info_id} not found",
                {"id": asset_info_id},
            )

        payload = schemas_out.AssetDetail(
            id=result.info.id,
            name=result.info.name,
            asset_hash=result.asset.hash if result.asset else None,
            size=int(result.asset.size_bytes)
            if result.asset and result.asset.size_bytes is not None
            else None,
            mime_type=result.asset.mime_type if result.asset else None,
            tags=result.tags,
            user_metadata=result.info.user_metadata or {},
            preview_id=result.info.preview_id,
            created_at=result.info.created_at,
            last_access_time=result.info.last_access_time,
        )
    except ValueError as e:
        return _build_error_response(
            404, "ASSET_NOT_FOUND", str(e), {"id": asset_info_id}
        )
    except Exception:
        logging.exception(
            "get_asset failed for asset_info_id=%s, owner_id=%s",
            asset_info_id,
            USER_MANAGER.get_request_user_id(request),
        )
        return _build_error_response(500, "INTERNAL", "Unexpected server error.")
    return web.json_response(payload.model_dump(mode="json"), status=200)


@ROUTES.get(f"/api/assets/{{id:{UUID_RE}}}/content")
async def download_asset_content(request: web.Request) -> web.Response:
    disposition = request.query.get("disposition", "attachment").lower().strip()
    if disposition not in {"inline", "attachment"}:
        disposition = "attachment"

    try:
        result = resolve_asset_for_download(
            asset_info_id=str(uuid.UUID(request.match_info["id"])),
            owner_id=USER_MANAGER.get_request_user_id(request),
        )
        abs_path = result.abs_path
        content_type = result.content_type
        filename = result.download_name
    except ValueError as ve:
        return _build_error_response(404, "ASSET_NOT_FOUND", str(ve))
    except NotImplementedError as nie:
        return _build_error_response(501, "BACKEND_UNSUPPORTED", str(nie))
    except FileNotFoundError:
        return _build_error_response(
            404, "FILE_NOT_FOUND", "Underlying file not found on disk."
        )

    quoted = (filename or "").replace("\r", "").replace("\n", "").replace('"', "'")
    cd = f"{disposition}; filename=\"{quoted}\"; filename*=UTF-8''{urllib.parse.quote(filename)}"

    file_size = os.path.getsize(abs_path)
    logging.info(
        "download_asset_content: path=%s, size=%d bytes (%.2f MB), content_type=%s, filename=%s",
        abs_path,
        file_size,
        file_size / (1024 * 1024),
        content_type,
        filename,
    )

    async def stream_file_chunks():
        chunk_size = 64 * 1024
        with open(abs_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    return web.Response(
        body=stream_file_chunks(),
        content_type=content_type,
        headers={
            "Content-Disposition": cd,
            "Content-Length": str(file_size),
        },
    )


@ROUTES.post("/api/assets/from-hash")
async def create_asset_from_hash_route(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
        body = schemas_in.CreateFromHashBody.model_validate(payload)
    except ValidationError as ve:
        return _build_validation_error_response("INVALID_BODY", ve)
    except Exception:
        return _build_error_response(
            400, "INVALID_JSON", "Request body must be valid JSON."
        )

    result = create_from_hash(
        hash_str=body.hash,
        name=body.name,
        tags=body.tags,
        user_metadata=body.user_metadata,
        owner_id=USER_MANAGER.get_request_user_id(request),
    )
    if result is None:
        return _build_error_response(
            404, "ASSET_NOT_FOUND", f"Asset content {body.hash} does not exist"
        )

    payload_out = schemas_out.AssetCreated(
        id=result.info.id,
        name=result.info.name,
        asset_hash=result.asset.hash,
        size=int(result.asset.size_bytes) if result.asset.size_bytes else None,
        mime_type=result.asset.mime_type,
        tags=result.tags,
        user_metadata=result.info.user_metadata or {},
        preview_id=result.info.preview_id,
        created_at=result.info.created_at,
        last_access_time=result.info.last_access_time,
        created_new=result.created_new,
    )
    return web.json_response(payload_out.model_dump(mode="json"), status=201)


def _delete_temp_file_if_exists(path: str | None) -> None:
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass


@ROUTES.post("/api/assets")
async def upload_asset(request: web.Request) -> web.Response:
    """Multipart/form-data endpoint for Asset uploads."""
    try:
        parsed = await parse_multipart_upload(request, check_hash_exists=asset_exists)
    except UploadError as e:
        return _build_error_response(e.status, e.code, e.message)

    owner_id = USER_MANAGER.get_request_user_id(request)

    try:
        spec = schemas_in.UploadAssetSpec.model_validate(
            {
                "tags": parsed.tags_raw,
                "name": parsed.provided_name,
                "user_metadata": parsed.user_metadata_raw,
                "hash": parsed.provided_hash,
            }
        )
    except ValidationError as ve:
        _delete_temp_file_if_exists(parsed.tmp_path)
        return _build_error_response(
            400, "INVALID_BODY", f"Validation failed: {ve.json()}"
        )

    if spec.tags and spec.tags[0] == "models":
        if (
            len(spec.tags) < 2
            or spec.tags[1] not in folder_paths.folder_names_and_paths
        ):
            _delete_temp_file_if_exists(parsed.tmp_path)
            category = spec.tags[1] if len(spec.tags) >= 2 else ""
            return _build_error_response(
                400, "INVALID_BODY", f"unknown models category '{category}'"
            )

    try:
        # Fast path: if a valid provided hash exists, create AssetInfo without writing anything
        if spec.hash and parsed.provided_hash_exists is True:
            result = create_from_hash(
                hash_str=spec.hash,
                name=spec.name or (spec.hash.split(":", 1)[1]),
                tags=spec.tags,
                user_metadata=spec.user_metadata or {},
                owner_id=owner_id,
            )
            if result is None:
                _delete_temp_file_if_exists(parsed.tmp_path)
                return _build_error_response(
                    404, "ASSET_NOT_FOUND", f"Asset content {spec.hash} does not exist"
                )
            _delete_temp_file_if_exists(parsed.tmp_path)
        else:
            # Otherwise, we must have a temp file path to ingest
            if not parsed.tmp_path or not os.path.exists(parsed.tmp_path):
                return _build_error_response(
                    404,
                    "ASSET_NOT_FOUND",
                    "Provided hash not found and no file uploaded.",
                )

            result = upload_from_temp_path(
                temp_path=parsed.tmp_path,
                name=spec.name,
                tags=spec.tags,
                user_metadata=spec.user_metadata or {},
                client_filename=parsed.file_client_name,
                owner_id=owner_id,
                expected_hash=spec.hash,
            )
    except AssetValidationError as e:
        _delete_temp_file_if_exists(parsed.tmp_path)
        return _build_error_response(400, e.code, str(e))
    except ValueError as e:
        _delete_temp_file_if_exists(parsed.tmp_path)
        return _build_error_response(400, "BAD_REQUEST", str(e))
    except HashMismatchError as e:
        _delete_temp_file_if_exists(parsed.tmp_path)
        return _build_error_response(400, "HASH_MISMATCH", str(e))
    except DependencyMissingError as e:
        _delete_temp_file_if_exists(parsed.tmp_path)
        return _build_error_response(503, "DEPENDENCY_MISSING", e.message)
    except Exception:
        _delete_temp_file_if_exists(parsed.tmp_path)
        logging.exception("upload_asset failed for owner_id=%s", owner_id)
        return _build_error_response(500, "INTERNAL", "Unexpected server error.")

    payload = schemas_out.AssetCreated(
        id=result.info.id,
        name=result.info.name,
        asset_hash=result.asset.hash,
        size=int(result.asset.size_bytes) if result.asset.size_bytes else None,
        mime_type=result.asset.mime_type,
        tags=result.tags,
        user_metadata=result.info.user_metadata or {},
        preview_id=result.info.preview_id,
        created_at=result.info.created_at,
        last_access_time=result.info.last_access_time,
        created_new=result.created_new,
    )
    status = 201 if result.created_new else 200
    return web.json_response(payload.model_dump(mode="json"), status=status)


@ROUTES.put(f"/api/assets/{{id:{UUID_RE}}}")
async def update_asset_route(request: web.Request) -> web.Response:
    asset_info_id = str(uuid.UUID(request.match_info["id"]))
    try:
        body = schemas_in.UpdateAssetBody.model_validate(await request.json())
    except ValidationError as ve:
        return _build_validation_error_response("INVALID_BODY", ve)
    except Exception:
        return _build_error_response(
            400, "INVALID_JSON", "Request body must be valid JSON."
        )

    try:
        result = update_asset_metadata(
            asset_info_id=asset_info_id,
            name=body.name,
            user_metadata=body.user_metadata,
            owner_id=USER_MANAGER.get_request_user_id(request),
        )
        payload = schemas_out.AssetUpdated(
            id=result.info.id,
            name=result.info.name,
            asset_hash=result.asset.hash if result.asset else None,
            tags=result.tags,
            user_metadata=result.info.user_metadata or {},
            updated_at=result.info.updated_at,
        )
    except (ValueError, PermissionError) as ve:
        return _build_error_response(
            404, "ASSET_NOT_FOUND", str(ve), {"id": asset_info_id}
        )
    except Exception:
        logging.exception(
            "update_asset failed for asset_info_id=%s, owner_id=%s",
            asset_info_id,
            USER_MANAGER.get_request_user_id(request),
        )
        return _build_error_response(500, "INTERNAL", "Unexpected server error.")
    return web.json_response(payload.model_dump(mode="json"), status=200)


@ROUTES.delete(f"/api/assets/{{id:{UUID_RE}}}")
async def delete_asset_route(request: web.Request) -> web.Response:
    asset_info_id = str(uuid.UUID(request.match_info["id"]))
    delete_content_param = request.query.get("delete_content")
    delete_content = (
        True
        if delete_content_param is None
        else delete_content_param.lower() not in {"0", "false", "no"}
    )

    try:
        deleted = delete_asset_reference(
            asset_info_id=asset_info_id,
            owner_id=USER_MANAGER.get_request_user_id(request),
            delete_content_if_orphan=delete_content,
        )
    except Exception:
        logging.exception(
            "delete_asset_reference failed for asset_info_id=%s, owner_id=%s",
            asset_info_id,
            USER_MANAGER.get_request_user_id(request),
        )
        return _build_error_response(500, "INTERNAL", "Unexpected server error.")

    if not deleted:
        return _build_error_response(
            404, "ASSET_NOT_FOUND", f"AssetInfo {asset_info_id} not found."
        )
    return web.Response(status=204)


@ROUTES.get("/api/tags")
async def get_tags(request: web.Request) -> web.Response:
    """
    GET request to list all tags based on query parameters.
    """
    query_map = dict(request.rel_url.query)

    try:
        query = schemas_in.TagsListQuery.model_validate(query_map)
    except ValidationError as e:
        return web.json_response(
            {
                "error": {
                    "code": "INVALID_QUERY",
                    "message": "Invalid query parameters",
                    "details": e.errors(),
                }
            },
            status=400,
        )

    rows, total = list_tags(
        prefix=query.prefix,
        limit=query.limit,
        offset=query.offset,
        order=query.order,
        include_zero=query.include_zero,
        owner_id=USER_MANAGER.get_request_user_id(request),
    )

    tags = [
        schemas_out.TagUsage(name=name, count=count, type=tag_type)
        for (name, tag_type, count) in rows
    ]
    payload = schemas_out.TagsList(
        tags=tags, total=total, has_more=(query.offset + len(tags)) < total
    )
    return web.json_response(payload.model_dump(mode="json"))


@ROUTES.post(f"/api/assets/{{id:{UUID_RE}}}/tags")
async def add_asset_tags(request: web.Request) -> web.Response:
    asset_info_id = str(uuid.UUID(request.match_info["id"]))
    try:
        json_payload = await request.json()
        data = schemas_in.TagsAdd.model_validate(json_payload)
    except ValidationError as ve:
        return _build_error_response(
            400,
            "INVALID_BODY",
            "Invalid JSON body for tags add.",
            {"errors": ve.errors()},
        )
    except Exception:
        return _build_error_response(
            400, "INVALID_JSON", "Request body must be valid JSON."
        )

    try:
        result = apply_tags(
            asset_info_id=asset_info_id,
            tags=data.tags,
            origin="manual",
            owner_id=USER_MANAGER.get_request_user_id(request),
        )
        payload = schemas_out.TagsAdd(
            added=result.added,
            already_present=result.already_present,
            total_tags=result.total_tags,
        )
    except (ValueError, PermissionError) as ve:
        return _build_error_response(
            404, "ASSET_NOT_FOUND", str(ve), {"id": asset_info_id}
        )
    except Exception:
        logging.exception(
            "add_tags_to_asset failed for asset_info_id=%s, owner_id=%s",
            asset_info_id,
            USER_MANAGER.get_request_user_id(request),
        )
        return _build_error_response(500, "INTERNAL", "Unexpected server error.")

    return web.json_response(payload.model_dump(mode="json"), status=200)


@ROUTES.delete(f"/api/assets/{{id:{UUID_RE}}}/tags")
async def delete_asset_tags(request: web.Request) -> web.Response:
    asset_info_id = str(uuid.UUID(request.match_info["id"]))
    try:
        json_payload = await request.json()
        data = schemas_in.TagsRemove.model_validate(json_payload)
    except ValidationError as ve:
        return _build_error_response(
            400,
            "INVALID_BODY",
            "Invalid JSON body for tags remove.",
            {"errors": ve.errors()},
        )
    except Exception:
        return _build_error_response(
            400, "INVALID_JSON", "Request body must be valid JSON."
        )

    try:
        result = remove_tags(
            asset_info_id=asset_info_id,
            tags=data.tags,
            owner_id=USER_MANAGER.get_request_user_id(request),
        )
        payload = schemas_out.TagsRemove(
            removed=result.removed,
            not_present=result.not_present,
            total_tags=result.total_tags,
        )
    except ValueError as ve:
        return _build_error_response(
            404, "ASSET_NOT_FOUND", str(ve), {"id": asset_info_id}
        )
    except Exception:
        logging.exception(
            "remove_tags_from_asset failed for asset_info_id=%s, owner_id=%s",
            asset_info_id,
            USER_MANAGER.get_request_user_id(request),
        )
        return _build_error_response(500, "INTERNAL", "Unexpected server error.")

    return web.json_response(payload.model_dump(mode="json"), status=200)


@ROUTES.post("/api/assets/seed")
async def seed_assets(request: web.Request) -> web.Response:
    """Trigger asset seeding for specified roots (models, input, output).

    Query params:
        wait: If "true", block until scan completes (synchronous behavior for tests)

    Returns:
        202 Accepted if scan started
        409 Conflict if scan already running
        200 OK with final stats if wait=true
    """
    try:
        payload = await request.json()
        roots = payload.get("roots", ["models", "input", "output"])
    except Exception:
        roots = ["models", "input", "output"]

    valid_roots = tuple(r for r in roots if r in ("models", "input", "output"))
    if not valid_roots:
        return _build_error_response(400, "INVALID_BODY", "No valid roots specified")

    wait_param = request.query.get("wait", "").lower()
    should_wait = wait_param in ("true", "1", "yes")

    started = asset_seeder.start(roots=valid_roots)
    if not started:
        return web.json_response({"status": "already_running"}, status=409)

    if should_wait:
        asset_seeder.wait()
        status = asset_seeder.get_status()
        return web.json_response(
            {
                "status": "completed",
                "progress": {
                    "scanned": status.progress.scanned if status.progress else 0,
                    "total": status.progress.total if status.progress else 0,
                    "created": status.progress.created if status.progress else 0,
                    "skipped": status.progress.skipped if status.progress else 0,
                },
                "errors": status.errors,
            },
            status=200,
        )

    return web.json_response({"status": "started"}, status=202)


@ROUTES.get("/api/assets/seed/status")
async def get_seed_status(request: web.Request) -> web.Response:
    """Get current scan status and progress."""
    status = asset_seeder.get_status()
    return web.json_response(
        {
            "state": status.state.value,
            "progress": {
                "scanned": status.progress.scanned,
                "total": status.progress.total,
                "created": status.progress.created,
                "skipped": status.progress.skipped,
            }
            if status.progress
            else None,
            "errors": status.errors,
        },
        status=200,
    )


@ROUTES.post("/api/assets/seed/cancel")
async def cancel_seed(request: web.Request) -> web.Response:
    """Request cancellation of in-progress scan."""
    cancelled = asset_seeder.cancel()
    if cancelled:
        return web.json_response({"status": "cancelling"}, status=200)
    return web.json_response({"status": "idle"}, status=200)


@ROUTES.post("/api/assets/prune")
async def mark_missing_assets(request: web.Request) -> web.Response:
    """Mark assets as missing when their cache states point to files outside all known root prefixes.

    This is a non-destructive soft-delete operation. Assets and their metadata
    are preserved, but cache states are flagged as missing. They can be restored
    if the file reappears in a future scan.

    Returns:
        200 OK with count of marked assets
        409 Conflict if a scan is currently running
    """
    marked = asset_seeder.mark_missing_outside_prefixes()
    if marked == 0 and asset_seeder.get_status().state.value != "IDLE":
        return web.json_response(
            {"status": "scan_running", "marked": 0},
            status=409,
        )
    return web.json_response({"status": "completed", "marked": marked}, status=200)
