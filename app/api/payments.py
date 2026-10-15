from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.deps import require_api_key
from app.core.db import SessionDep
from app.repositories.payments import fetch_payment, insert_payment
from app.schemas import PaymentCreate, PaymentResponse

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


@router.post("", response_model=PaymentResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    response: Response,
    payload: PaymentCreate,
    session: SessionDep,
    idempotency_key: str = Header(alias="Idempotency-Key"),
) -> PaymentResponse:
    payment, created = await insert_payment(session, payload, idempotency_key)
    await session.commit()
    if not created:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replay"] = "true"
    return PaymentResponse.model_validate(payment)


@router.get("/{payment_id}", response_model=PaymentResponse)
async def get_payment(payment_id: UUID, session: SessionDep) -> PaymentResponse:
    payment = await fetch_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentResponse.model_validate(payment)
