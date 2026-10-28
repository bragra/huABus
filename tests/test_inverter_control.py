"""Tests for the opt-in persistent PV output power limit control."""

from types import SimpleNamespace

import pytest
from bridge.inverter_control import (
    cap_writable,
    get_pmax,
    handle_power_limit_command,
    parse_power_limit,
    set_power_limit,
)
from huawei_solar import register_names as rn
from huawei_solar.exceptions import WriteException


def register_value(value):
    return SimpleNamespace(value=value)


@pytest.mark.parametrize("payload", [b"2000", "2000", b" 2000.0 "])
def test_parse_power_limit_accepts_integer_watts(payload):
    assert parse_power_limit(payload) == 2000


@pytest.mark.parametrize("payload", [b"", b"abc", b"1.5", b"NaN", b"Infinity", b"\xff"])
def test_parse_power_limit_rejects_invalid_values(payload):
    with pytest.raises(ValueError):
        parse_power_limit(payload)


@pytest.mark.asyncio
async def test_get_pmax_requires_a_positive_value(mock_client):
    mock_client.get.return_value = register_value(0)
    with pytest.raises(ValueError, match="invalid P_MAX"):
        await get_pmax(mock_client)


@pytest.mark.asyncio
async def test_cap_writable_accepts_decoded_on_grid_status(mock_client):
    mock_client.get.return_value = register_value("On-grid")
    assert await cap_writable(mock_client) == (True, "On-grid")


@pytest.mark.asyncio
async def test_set_power_limit_writes_and_verifies_persistent_cap(mock_client):
    mock_client.get.side_effect = [
        register_value(10000),
        register_value(0x0200),
        register_value(2000),
    ]
    mock_client.set.return_value = True

    assert await set_power_limit(mock_client, b"2000.0") == 2000
    mock_client.set.assert_awaited_once_with(rn.ACTIVE_POWER_FIXED_VALUE_DERATING, 2000)


@pytest.mark.asyncio
@pytest.mark.parametrize("watts", [-1, 10001])
async def test_set_power_limit_rejects_values_outside_pmax(mock_client, watts):
    mock_client.get.return_value = register_value(10000)

    with pytest.raises(ValueError, match="between 0 and 10000"):
        await set_power_limit(mock_client, str(watts))
    mock_client.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_set_power_limit_refuses_night_standby(mock_client):
    mock_client.get.side_effect = [
        register_value(10000),
        register_value(0xA000),
    ]

    with pytest.raises(ValueError, match="does not accept"):
        await set_power_limit(mock_client, "2000")
    mock_client.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_set_power_limit_checks_readback(mock_client):
    mock_client.get.side_effect = [
        register_value(10000),
        register_value("Grid Connection"),
        register_value(1500),
    ]
    mock_client.set.return_value = True

    with pytest.raises(ValueError, match="readback is 1500"):
        await set_power_limit(mock_client, "2000")


@pytest.mark.asyncio
async def test_command_handler_contains_write_errors(mock_client):
    mock_client.get.side_effect = [
        register_value(10000),
        register_value(0x0200),
    ]
    mock_client.set.side_effect = WriteException("denied")

    assert await handle_power_limit_command(mock_client, "2000") is False


@pytest.mark.asyncio
async def test_command_handler_returns_true_on_success(mock_client):
    mock_client.get.side_effect = [
        register_value(10000),
        register_value(0x0203),
        register_value(0),
    ]
    mock_client.set.return_value = True

    assert await handle_power_limit_command(mock_client, "0") is True
    mock_client.set.assert_awaited_once_with(rn.ACTIVE_POWER_FIXED_VALUE_DERATING, 0)
