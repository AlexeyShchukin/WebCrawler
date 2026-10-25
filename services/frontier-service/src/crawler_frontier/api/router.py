"""Internal API routes for Frontier's crawl and fetch-lease operations."""

from typing import Annotated
from uuid import UUID

from crawler_contracts import (
    FetchRetryRequestedEvent,
    PageFailedEvent,
    PageFetchedEvent,
)
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from crawler_frontier.application.crawl_service import CrawlNotFoundError, CrawlService
from crawler_frontier.application.fetch_lease_service import (
    FetchLeaseService,
    LeaseLostError,
)
from crawler_frontier.database import get_session
from crawler_frontier.domain.url_policy import UrlValidationError
from crawler_frontier.schemas import (
    CrawlStatusResponse,
    CreateCrawlRequest,
    CreatedCrawlResponse,
    FetchLeaseRenewRequest,
    FetchLeaseRequest,
    FetchLeaseResponse,
    FetchLeaseSubmitRequest,
)

router = APIRouter()


@router.post("/internal/fetch-leases/claim", response_model=FetchLeaseResponse)
async def claim_fetch_lease(
    request: FetchLeaseRequest,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> FetchLeaseResponse:
    try:
        grant = await FetchLeaseService(
            session, http_request.app.state.frontier_policy
        ).claim(**request.model_dump())
    except LeaseLostError as error:
        raise HTTPException(status_code=409, detail="lease_lost") from error
    return FetchLeaseResponse(owner_token=grant.owner_token, lease_until=grant.lease_until)


@router.post("/internal/fetch-leases/renew", response_model=FetchLeaseResponse)
async def renew_fetch_lease(
    request: FetchLeaseRenewRequest,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> FetchLeaseResponse:
    try:
        grant = await FetchLeaseService(session, http_request.app.state.frontier_policy).renew(
            crawl_id=request.crawl_id,
            url_id=request.url_id,
            fetch_attempt=request.fetch_attempt,
            owner_token=request.owner_token,
        )
    except LeaseLostError as error:
        raise HTTPException(status_code=409, detail="lease_lost") from error
    return FetchLeaseResponse(owner_token=grant.owner_token, lease_until=grant.lease_until)


@router.post("/internal/fetch-leases/submit", status_code=status.HTTP_204_NO_CONTENT)
async def submit_fetch_lease(
    request: FetchLeaseSubmitRequest,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> None:
    event = next(
        (
            parsed_event
            for event_type in (PageFetchedEvent, PageFailedEvent, FetchRetryRequestedEvent)
            if (parsed_event := _validate_fetch_outcome(event_type, request.event)) is not None
        ),
        None,
    )
    if event is None:
        raise HTTPException(status_code=422, detail="invalid fetch outcome")
    if (event.crawl_id, event.url_id, event.fetch_attempt) != (
        request.crawl_id,
        request.url_id,
        request.fetch_attempt,
    ):
        raise HTTPException(status_code=422, detail="outcome identity mismatch")
    try:
        await FetchLeaseService(session, http_request.app.state.frontier_policy).submit(
            event, owner_token=request.owner_token
        )
    except LeaseLostError as error:
        raise HTTPException(status_code=409, detail="lease_lost") from error


@router.post(
    "/internal/crawls",
    response_model=CreatedCrawlResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_crawl(
    payload: CreateCrawlRequest,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> CreatedCrawlResponse:
    """Create a crawl and its initial fetch work inside Frontier's database."""
    try:
        created = await CrawlService(session, http_request.app.state.frontier_policy).create_crawl(
            payload.seed_urls,
            additional_allowed_hosts=payload.additional_allowed_hosts,
        )
    except UrlValidationError as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(error),
        ) from error
    return CreatedCrawlResponse(crawl_id=created.crawl_id)


@router.get("/internal/crawls/{crawl_id}", response_model=CrawlStatusResponse)
async def get_crawl_status(
    crawl_id: UUID,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> CrawlStatusResponse:
    """Report a crawl's derived scheduling status without mutating it."""
    try:
        crawl_status = await CrawlService(
            session, http_request.app.state.frontier_policy
        ).get_crawl_status(crawl_id=crawl_id)
    except CrawlNotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Crawl not found") from error
    return CrawlStatusResponse(
        crawl_id=crawl_status.crawl_id,
        state=crawl_status.state,
        url_counts={url_status.value: count for url_status, count in crawl_status.url_counts.items()},
    )


def _validate_fetch_outcome(
        event_type: type[PageFetchedEvent | PageFailedEvent | FetchRetryRequestedEvent],
        event: dict[str, object]
) -> PageFetchedEvent | PageFailedEvent | FetchRetryRequestedEvent | None:
    try:
        return event_type.model_validate(event)
    except ValidationError:
        return None
