"""Opt-in control of the inverter's persistent PV output power limit."""

import logging
from decimal import Decimal, InvalidOperation

from huawei_solar import AsyncHuaweiSolarClient
from huawei_solar import register_names as rn
from huawei_solar.exceptions import HuaweiSolarException
from tmodbus.exceptions import TModbusError

logger = logging.getLogger("huawei.control")

# Device status codes under which the inverter accepts cap writes. Writes are
# rejected in 0xA000 ("Standby: no irradiation") with Modbus exception 0x01.
WRITABLE_STATUSES = {0x0200, 0x0201, 0x0202, 0x0203}

CONTROL_EXCEPTIONS: tuple[type[BaseException], ...] = (
    HuaweiSolarException,
    TModbusError,
    TimeoutError,
    ConnectionError,
    ValueError,
)


def parse_power_limit(payload: bytes | str) -> int:
    """Parse an integer wattage, including Home Assistant's ``2000.0`` form."""
    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        value = Decimal(text.strip())
    except (UnicodeDecodeError, InvalidOperation) as exc:
        raise ValueError("power limit must be an integer number of watts") from exc

    if not value.is_finite() or value != value.to_integral_value():
        raise ValueError("power limit must be a finite integer number of watts")
    return int(value)


async def get_pmax(client: AsyncHuaweiSolarClient) -> int:
    """Read and validate the inverter's maximum active power."""
    value = (await client.get(rn.P_MAX)).value
    try:
        pmax = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid P_MAX value: {value!r}") from exc
    if pmax <= 0:
        raise ValueError(f"invalid P_MAX value: {pmax}")
    return pmax


async def get_device_status(client: AsyncHuaweiSolarClient) -> tuple[int | None, str]:
    """Return the raw device status, when available, and its description."""
    value = (await client.get(rn.DEVICE_STATUS)).value
    if isinstance(value, str):
        return None, value
    try:
        raw = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid device status: {value!r}") from exc
    return raw, str(value)


async def cap_writable(client: AsyncHuaweiSolarClient) -> tuple[bool, str]:
    """Check whether the inverter is in a state that accepts cap writes."""
    raw, description = await get_device_status(client)
    if raw is None:
        writable = description.startswith(("On-grid", "Grid Connection"))
    else:
        writable = raw in WRITABLE_STATUSES
    return writable, description


async def set_power_limit(client: AsyncHuaweiSolarClient, payload: bytes | str) -> int:
    """Set and verify the persistent output cap, returning the applied watts."""
    watts = parse_power_limit(payload)
    pmax = await get_pmax(client)
    if not 0 <= watts <= pmax:
        raise ValueError(f"power limit must be between 0 and {pmax} W (got {watts})")

    writable, status = await cap_writable(client)
    if not writable:
        raise ValueError(f"inverter does not accept power limit writes in state '{status}'")

    if not await client.set(rn.ACTIVE_POWER_FIXED_VALUE_DERATING, watts):
        raise ValueError(f"inverter did not confirm power limit write of {watts} W")

    applied = int((await client.get(rn.ACTIVE_POWER_FIXED_VALUE_DERATING)).value)
    if applied != watts:
        raise ValueError(f"power limit readback is {applied} W instead of requested {watts} W")

    logger.info("✅ Persistent PV output power limit set to %d W", applied)
    return applied


async def handle_power_limit_command(client: AsyncHuaweiSolarClient, payload: bytes | str) -> bool:
    """Handle one MQTT command without interrupting the bridge's polling loop."""
    try:
        await set_power_limit(client, payload)
    except CONTROL_EXCEPTIONS as exc:
        logger.error("❌ PV output power limit command failed: %s", exc)
        return False
    return True
