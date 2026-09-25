from fastapi import APIRouter, Depends, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import verify_corrections_key
from app.db.session import get_db
from app.schemas.correction import (
    TariffCorrectionConflict,
    TariffCorrectionRequest,
    TariffCorrectionResponse,
)
from app.services.corrections import (
    CorrectionError,
    apply_correction,
    payload_sha256,
    utility_lock,
)

router = APIRouter()


@router.post(
    "/tariff-corrections",
    status_code=201,
    response_model=TariffCorrectionResponse,
    dependencies=[Depends(verify_corrections_key)],
    responses={
        200: {"model": TariffCorrectionResponse, "description": "Replay of an earlier request with the same idempotency_key"},
        401: {"description": "Missing or wrong X-Corrections-Key"},
        404: {"description": "Utility or target tariff not found"},
        409: {"model": TariffCorrectionConflict, "description": "Target no longer live, name clash, idempotency key reuse, or refresh in progress"},
        503: {"description": "TARIFF_CORRECTIONS_API_KEY not configured"},
    },
)
async def apply_tariff_correction(
    body: TariffCorrectionRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """Apply a manual tariff correction that Mysa has **already approved**.

    Soft-supersede only: the new row becomes live, the predecessor keeps its
    components and is superseded with ``supersede_reason='manual'`` (or
    retired with ``manual_retire``). The new row is pinned to
    ``evidence.source_url`` so scraper refreshes hold instead of overwriting
    it; a later change to that document is verified automatically.

    Idempotent on ``idempotency_key``: a replay returns 200 with the original
    result (``replayed: true``); the same key with a different payload is 409.
    """
    sha = payload_sha256(body)
    try:
        with utility_lock(body.target.utility_id):
            result = await db.run_sync(lambda s: apply_correction(s, body, payload_sha=sha))
            await db.commit()
    except CorrectionError as e:
        await db.rollback()
        return JSONResponse(status_code=e.status, content=e.body)
    except IntegrityError:
        # Concurrent request with the same idempotency_key won the insert.
        await db.rollback()
        try:
            result = await db.run_sync(lambda s: apply_correction(s, body, payload_sha=sha))
        except CorrectionError as e:
            return JSONResponse(status_code=e.status, content=e.body)
    if result["replayed"]:
        response.status_code = 200
    return result
