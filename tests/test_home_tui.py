"""Tests for the top-level Home menu app: routing to laps and races."""

from unittest.mock import MagicMock, patch

import pytest

from lemongrass._home_tui import HomeScreen, LemongrassApp
from lemongrass._laps_tui import PickerScreen
from lemongrass._races_tui import RacesBrowserScreen


@pytest.mark.asyncio
async def test_home_opens_on_menu():
    app = LemongrassApp(MagicMock())
    async with app.run_test() as pilot:
        await pilot.pause()
        assert isinstance(app.screen, HomeScreen)


@pytest.mark.asyncio
async def test_home_routes_to_races():
    app = LemongrassApp(MagicMock())
    with patch('lemongrass._races_tui._influx.influx_token_present', return_value=False):
        async with app.run_test() as pilot:
            await pilot.pause()
            app.open_races()
            await pilot.pause()
            assert isinstance(app.screen, RacesBrowserScreen)


@pytest.mark.asyncio
async def test_home_routes_to_laps():
    app = LemongrassApp(MagicMock())
    async with app.run_test() as pilot:
        await pilot.pause()
        app.start_laps()
        await pilot.pause()
        assert isinstance(app.screen, PickerScreen)
