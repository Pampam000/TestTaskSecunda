from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.consumer.gateway import EmulatedGateway
from app.consumer.webhook import WebhookSender
from app.models.payment import Payment, PaymentStatus

PAYMENT_SUCCEEDED_EVENT = "payment.succeeded"
PAYMENT_FAILED_EVENT = "payment.failed"


async def process_payment(
    session: AsyncSession,
    payment_id: UUID,
    gateway: EmulatedGateway,
    webhook_sender: WebhookSender | None,
) -> None:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        return
    if payment.status == PaymentStatus.PENDING:
        await settle_payment(session, payment, gateway)
    await session.commit()
    await notify(payment, webhook_sender)


async def settle_payment(
    session: AsyncSession, payment: Payment, gateway: EmulatedGateway
) -> None:
    charged = await gateway.charge()
    payment.status = PaymentStatus.SUCCEEDED if charged else PaymentStatus.FAILED
    payment.processed_at = datetime.now(UTC)
    await session.flush()


async def notify(payment: Payment, webhook_sender: WebhookSender | None) -> None:
    if webhook_sender is None or payment.webhook_url is None:
        return
    delivered = (
        PAYMENT_SUCCEEDED_EVENT
        if payment.status == PaymentStatus.SUCCEEDED
        else PAYMENT_FAILED_EVENT
    )
    await webhook_sender.deliver(
        payment.webhook_url,
        {"payment_id": str(payment.id), "status": payment.status.value, "event": delivered},
    )
