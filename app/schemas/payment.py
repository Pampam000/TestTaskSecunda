from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models.payment import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0)
    currency: Currency
    description: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    webhook_url: str | None = None

    @field_validator("amount")
    @classmethod
    def validate_scale(cls, value: Decimal) -> Decimal:
        if value % Decimal("0.01"):
            raise ValueError("amount allows at most 2 decimal places")
        return value


class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str | None
    metadata_json: dict[str, Any]
    webhook_url: str | None
    created_at: datetime
    processed_at: datetime | None
