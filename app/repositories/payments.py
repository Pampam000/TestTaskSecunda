from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.topology import PAYMENT_CREATED
from app.models.outbox import OutboxMessage
from app.models.payment import Payment
from app.schemas import PaymentCreate


async def insert_payment(
    session: AsyncSession, payload: PaymentCreate, idempotency_key: str
) -> tuple[Payment, bool]:
    payment = Payment(**payload.model_dump(), idempotency_key=idempotency_key)
    session.add(payment)
    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        found = await session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        return found.scalar_one(), False
    session.add(
        OutboxMessage(
            aggregate_id=payment.id,
            event_type=PAYMENT_CREATED,
            payload={"payment_id": str(payment.id)},
        )
    )
    return payment, True


async def fetch_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
