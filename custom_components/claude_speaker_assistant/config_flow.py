"""Config Flow fuer Claude Speaker Assistant."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult

from .const import (
    CONF_CLAUDE_BINARY,
    CONF_IDLE_TIMEOUT,
    CONF_SSH_HOST,
    CONF_SSH_KEY_PATH,
    CONF_SSH_PORT,
    CONF_SSH_USERNAME,
    DEFAULT_CLAUDE_BINARY,
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_SSH_PORT,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_SSH_HOST): str,
        vol.Optional(CONF_SSH_PORT, default=DEFAULT_SSH_PORT): int,
        vol.Required(CONF_SSH_USERNAME): str,
        vol.Required(CONF_SSH_KEY_PATH): str,
        vol.Optional(CONF_CLAUDE_BINARY, default=DEFAULT_CLAUDE_BINARY): str,
        vol.Optional(CONF_IDLE_TIMEOUT, default=DEFAULT_IDLE_TIMEOUT): int,
    }
)


class ClaudeSpeakerAssistantConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Config Flow: SSH-Zugangsdaten zur Claude-Code-Maschine abfragen."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Einziger Schritt: Verbindungsdaten abfragen.

        TODO (noch nicht getestet, siehe README): hier waere ein echter
        SSH-Verbindungstest sinnvoll, bevor der Eintrag angelegt wird.
        """
        errors: dict[str, str] = {}

        if user_input is not None:
            unique_id = f"{user_input[CONF_SSH_HOST]}:{user_input[CONF_SSH_USERNAME]}"
            await self.async_set_unique_id(unique_id)
            self._abort_if_unique_id_configured()

            return self.async_create_entry(
                title=f"Claude Speaker Assistant ({user_input[CONF_SSH_HOST]})",
                data=user_input,
            )

        return self.async_show_form(
            step_id="user", data_schema=STEP_USER_DATA_SCHEMA, errors=errors
        )
