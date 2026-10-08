import hashlib
import json
from datetime import datetime, timezone

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .errors import CouponError
from .models import (
    Coupon,
    CouponType,
    IdempotencyRecord,
    Order,
    OrderStatus,
    Redemption,
    RedemptionStatus,
)


# =========================================================
# HELPER
# =========================================================

def utc_now():
    return datetime.now(timezone.utc).replace(
        tzinfo=None
    )


def create_fingerprint(payload: dict) -> str:

    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        canonical.encode()
    ).hexdigest()


# =========================================================
# GET COUPON WITH DATABASE LOCK
# =========================================================

def get_coupon_for_update(
    db: Session,
    code: str,
):

    coupon = (
        db.execute(
            select(Coupon)
            .where(Coupon.code == code)
            .with_for_update()
        )
        .scalars()
        .first()
    )

    if coupon is None:

        raise CouponError(
            "UNKNOWN_CODE",
            "coupon code does not exist",
            404,
        )

    return coupon


# =========================================================
# REDEEM COUPON
# =========================================================

def redeem_coupon(
    db: Session,
    idempotency_key: str,
    code: str,
    customer_id: str,
    order_id: str,
):

    request_data = {
        "code": code,
        "customer_id": customer_id,
        "order_id": order_id,
    }

    request_hash = create_fingerprint(
        request_data
    )

    # -----------------------------------------------------
    # STEP 1: CHECK IDEMPOTENCY
    # -----------------------------------------------------

    existing_request = (
        db.execute(
            select(IdempotencyRecord)
            .where(
                IdempotencyRecord.idempotency_key
                == idempotency_key
            )
        )
        .scalars()
        .first()
    )

    if existing_request:

        # Same key but different request
        if (
            existing_request.request_fingerprint
            != request_hash
        ):

            raise CouponError(
                "IDEMPOTENCY_KEY_REUSED",
                "same Idempotency-Key was used for a different request",
                409,
            )

        response = json.loads(
            existing_request.response_json
        )

        response["replay"] = True

        return response

    # -----------------------------------------------------
    # STEP 2: LOCK COUPON ROW
    # -----------------------------------------------------

    coupon = get_coupon_for_update(
        db,
        code,
    )

    # -----------------------------------------------------
    # STEP 3: GET MYSQL TIME
    # -----------------------------------------------------

    database_time = db.execute(
        text("SELECT UTC_TIMESTAMP(6)")
    ).scalar_one()

    # -----------------------------------------------------
    # STEP 4: CHECK EXPIRY
    # -----------------------------------------------------

    if database_time >= coupon.expires_at:

        raise CouponError(
            "EXPIRED",
            "coupon has expired",
            409,
        )

    # -----------------------------------------------------
    # STEP 5: CHECK GLOBAL REDEMPTION LIMIT
    # -----------------------------------------------------

    if (
        coupon.redeemed_count
        >= coupon.max_redemptions
    ):

        raise CouponError(
            "NO_REDEMPTIONS_LEFT",
            "coupon has no redemptions remaining",
            409,
        )

    # -----------------------------------------------------
    # STEP 6: GET / CREATE ORDER
    # -----------------------------------------------------

    order = (
        db.execute(
            select(Order)
            .where(Order.order_id == order_id)
            .with_for_update()
        )
        .scalars()
        .first()
    )

    if order is None:

        order = Order(
            order_id=order_id,
            status=OrderStatus.ACTIVE,
        )

        db.add(order)

        db.flush()

    elif order.status == OrderStatus.CANCELLED:

        raise CouponError(
            "ORDER_CANCELLED",
            "order has already been cancelled",
            409,
        )

    # -----------------------------------------------------
    # STEP 7: CHECK ORDER ALREADY HAS COUPON
    # -----------------------------------------------------

    existing_redemption = (
        db.execute(
            select(Redemption)
            .where(
                Redemption.order_id
                == order.id
            )
            .with_for_update()
        )
        .scalars()
        .first()
    )

    if existing_redemption:

        raise CouponError(
            "ORDER_ALREADY_REDEEMED",
            "this order already has a coupon redemption",
            409,
        )

    # -----------------------------------------------------
    # STEP 8: STANDARD COUPON CUSTOMER CHECK
    # -----------------------------------------------------

    if coupon.type == CouponType.STANDARD:

        existing_customer_redemption = (
            db.execute(
                select(Redemption)
                .where(
                    Redemption.coupon_id
                    == coupon.id,

                    Redemption.customer_id
                    == customer_id,

                    Redemption.status
                    == RedemptionStatus.ACTIVE,
                )
                .with_for_update()
            )
            .scalars()
            .first()
        )

        if existing_customer_redemption:

            raise CouponError(
                "ALREADY_USED",
                "standard coupon was already redeemed by this customer",
                409,
            )

    # -----------------------------------------------------
    # STEP 9: CREATE REDEMPTION
    # -----------------------------------------------------

    redemption = Redemption(
        coupon_id=coupon.id,
        order_id=order.id,
        customer_id=customer_id,
        status=RedemptionStatus.ACTIVE,
        redeemed_at=database_time,
    )

    db.add(redemption)

    # -----------------------------------------------------
    # STEP 10: INCREMENT COUNT
    # -----------------------------------------------------

    coupon.redeemed_count += 1

    remaining = (
        coupon.max_redemptions
        - coupon.redeemed_count
    )

    # -----------------------------------------------------
    # STEP 11: STORE IDEMPOTENCY RESULT
    # -----------------------------------------------------

    response = {
        "success": True,
        "remaining": remaining,
        "replay": False,
    }

    idempotency_record = IdempotencyRecord(
        idempotency_key=idempotency_key,
        request_fingerprint=request_hash,
        success=True,
        response_json=json.dumps(
            response,
            separators=(",", ":"),
        ),
    )

    db.add(
        idempotency_record
    )

    return response


# =========================================================
# CANCEL ORDER
# =========================================================

def cancel_order(
    db: Session,
    order_id: str,
):

    # -----------------------------------------------------
    # STEP 1: LOCK ORDER
    # -----------------------------------------------------

    order = (
        db.execute(
            select(Order)
            .where(
                Order.order_id
                == order_id
            )
            .with_for_update()
        )
        .scalars()
        .first()
    )

    # Order doesn't exist
    if order is None:

        return {
            "success": True,
            "already_cancelled": True,
            "refunded": False,
        }

    # -----------------------------------------------------
    # STEP 2: ALREADY CANCELLED
    # -----------------------------------------------------

    if order.status == OrderStatus.CANCELLED:

        return {
            "success": True,
            "already_cancelled": True,
            "refunded": False,
        }

    # -----------------------------------------------------
    # STEP 3: FIND REDEMPTION
    # -----------------------------------------------------

    redemption = (
        db.execute(
            select(Redemption)
            .where(
                Redemption.order_id
                == order.id
            )
            .with_for_update()
        )
        .scalars()
        .first()
    )

    # -----------------------------------------------------
    # NO COUPON
    # -----------------------------------------------------

    if redemption is None:

        order.status = OrderStatus.CANCELLED

        order.cancelled_at = utc_now()

        return {
            "success": True,
            "already_cancelled": False,
            "refunded": False,
        }

    # -----------------------------------------------------
    # LOCK COUPON
    # -----------------------------------------------------

    coupon = (
        db.execute(
            select(Coupon)
            .where(
                Coupon.id
                == redemption.coupon_id
            )
            .with_for_update()
        )
        .scalars()
        .one()
    )

    # -----------------------------------------------------
    # RETURN SLOT ONLY ONCE
    # -----------------------------------------------------

    if (
        redemption.status
        == RedemptionStatus.ACTIVE
    ):

        redemption.status = (
            RedemptionStatus.CANCELLED
        )

        redemption.cancelled_at = utc_now()

        if coupon.redeemed_count <= 0:

            raise CouponError(
                "COUNT_INVARIANT_BROKEN",
                "redeemed_count cannot become negative",
                500,
            )

        coupon.redeemed_count -= 1

        refunded = True

    else:

        refunded = False

    # -----------------------------------------------------
    # CANCEL ORDER
    # -----------------------------------------------------

    order.status = OrderStatus.CANCELLED

    order.cancelled_at = utc_now()

    return {
        "success": True,
        "already_cancelled": False,
        "refunded": refunded,
    }