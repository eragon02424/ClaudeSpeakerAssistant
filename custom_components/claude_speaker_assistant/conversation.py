"""Conversation-Plattform fuer Claude Speaker Assistant.

Architektur (siehe Second-Brain-Projekt "claude-smart-home-zentrale", Entscheidungen vom 2026-09-28/29/30):
- Claude Code oeffnet selbst keinen Netzwerk-Port; die SSH-Verbindung uebernimmt den Transport.
- Ein einzelner Claude-Code-Prozess laeuft im Streaming-JSON-Modus und bleibt zwischen
  mehreren Sprachanfragen offen (kein Neustart pro Anfrage).
- Nach CONF_IDLE_TIMEOUT Sekunden ohne echte Anfrage wird die Sitzung PROAKTIV im
  Hintergrund neu aufgebaut (seit 2026-10-03, siehe _schedule_idle_rebuild /
  _handle_idle_rebuild) - nicht erst traege bei der naechsten Anfrage. WICHTIG
  (Praezisierung vom 2026-10-03): Der Timer laeuft NICHT endlos alle CONF_IDLE_TIMEOUT
  Sekunden weiter, wenn keine echten Nachrichten mehr kommen. Er wird nur durch eine
  echte Sprachanfrage (async_process) gestartet bzw. neu gestartet. Feuert er einmal
  proaktiv (_handle_idle_rebuild), baut er die Sitzung neu auf und stoppt danach, bis
  wieder eine echte Nachricht reinkommt. async_added_to_hass baut beim HA-Start direkt
  eine erste warme Sitzung auf, startet den Timer dabei aber bewusst NICHT - der Timer
  beginnt erst mit der ersten echten Nachricht.
- Abbruch laeuft ueber das dokumentierte control_request/interrupt-Kommando des
  Streaming-JSON-Protokolls (kein Agent SDK noetig), mit hartem Kill als Sicherheitsnetz.
- Bewusst immer der volle MCP-Serverausbau (keine --mcp-config-Reduktion): im echten Test
  lag die Antwortzeit trotzdem unter 2 Sekunden, eine Reduktion bringt keinen Mehrwert.
- --permission-mode bypassPermissions ist bewusst gesetzt (Entscheidung vom 2026-10-02):
  Im headless Streaming-JSON-Modus (-p, kein TTY) kann Claude Code keine interaktive
  Rueckfrage stellen, auch nicht beim ersten Start eines MCP-Servers. Ohne diesen Flag
  wuerde der MCP-Zugriff schlicht verweigert. Der Sprachassistent soll uneingeschraenkt
  auf alle MCP-Server zugreifen koennen, ohne dass einer gesperrt ist.
- --model claude-haiku-4-5-20251001 ist testweise gesetzt (Entscheidung vom 2026-10-02):
  kleineres/schnelleres Modell zum Ausprobieren anstelle des Standardmodells.
- _handle_abort pausiert seit 2026-10-02 zusaetzlich ALLE Medienwiedergaben (entity_id: all),
  nicht nur die ReSpeaker-eigene. Bewusst NICHT als lokaler HA-Intent gebaut: ein lokaler
  Intent fuer "Abbruch"/"stopp" wuerde wegen prefer_local_intents Vorrang vor diesem
  Conversation-Agent bekommen und so den Interrupt einer laufenden Claude-Code-Anfrage
  verhindern. Beides soll gleichzeitig passieren, deshalb liegt es hier im selben Codepfad.

Getestet und funktionsfaehig seit 2026-09-30 (echte Anfrage ueber die Assist-Pipeline
lieferte eine korrekte Antwort in ca. 1.7s). Proaktiver Idle-Rebuild am 2026-10-03 echt
getestet (23 Minuten Pause, Session wurde eigenstaendig vor der naechsten Anfrage erneuert).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Callable

import asyncssh

from homeassistant.components import conversation
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import MATCH_ALL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers import intent as ha_intent
from homeassistant.helpers.event import async_call_later

from .const import (
    CONF_ABORT_PHRASES,
    CONF_CLAUDE_BINARY,
    CONF_IDLE_TIMEOUT,
    CONF_SSH_HOST,
    CONF_SSH_KEY_PATH,
    CONF_SSH_PORT,
    CONF_SSH_USERNAME,
    DEFAULT_ABORT_PHRASES,
    DEFAULT_CLAUDE_BINARY,
    DEFAULT_IDLE_TIMEOUT,
    DOMAIN,
    INTERRUPT_GRACE_PERIOD,
)

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Registriert die Conversation-Entity."""
    data = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([ClaudeSpeakerConversationEntity(entry, data)])


class ClaudeSpeakerConversationEntity(conversation.ConversationEntity):
    """Leitet Sprachanfragen per SSH an eine entfernte Claude-Code-Instanz weiter."""

    _attr_has_entity_name = True
    _attr_name = None

    @property
    def supported_languages(self):
        """Return a list of supported languages."""
        return MATCH_ALL

    def __init__(self, entry: ConfigEntry, data: dict[str, Any]) -> None:
        self._entry = entry
        self._data = data
        self._attr_unique_id = f"{entry.entry_id}_conversation"

        self._ssh_conn: asyncssh.SSHClientConnection | None = None
        self._process: asyncssh.SSHClientProcess | None = None
        self._lock = asyncio.Lock()
        self._last_activity: float = 0.0
        self._idle_rebuild_unsub: Callable[[], None] | None = None

        self._abort_phrases = [
            p.strip().lower()
            for p in data.get(CONF_ABORT_PHRASES, DEFAULT_ABORT_PHRASES)
        ]

    @property
    def device_info(self) -> dict[str, Any]:
        return {
            "identifiers": {(DOMAIN, self._entry.entry_id)},
            "name": "Claude Speaker Assistant",
            "manufacturer": "eragon02424",
        }

    async def async_added_to_hass(self) -> None:
        """Baut beim Start von Home Assistant direkt eine warme Sitzung auf.

        Startet bewusst NICHT den Idle-Rebuild-Timer - der beginnt erst mit der
        ersten echten Sprachnachricht (siehe async_process), damit ohne jegliche
        Nutzung nicht alle CONF_IDLE_TIMEOUT Sekunden unnoetig neu verbunden wird.
        """
        await super().async_added_to_hass()
        try:
            async with self._lock:
                await self._ensure_session()
        except (asyncssh.Error, OSError) as err:
            _LOGGER.warning("Warmstart der Claude-Code-Sitzung fehlgeschlagen (%s).", err)

    async def async_will_remove_from_hass(self) -> None:
        """Raeumt Timer und Sitzung auf, wenn die Entity entfernt wird."""
        if self._idle_rebuild_unsub is not None:
            self._idle_rebuild_unsub()
            self._idle_rebuild_unsub = None
        await self._teardown()

    def _schedule_idle_rebuild(self) -> None:
        """Plant einen proaktiven Sitzungsneuaufbau CONF_IDLE_TIMEOUT Sekunden ab jetzt.

        Wird NUR von einer echten Sprachnachricht (async_process) aufgerufen, nie
        automatisch verkettet - siehe _handle_idle_rebuild.
        """
        if self._idle_rebuild_unsub is not None:
            self._idle_rebuild_unsub()
        idle_timeout = self._data.get(CONF_IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT)
        self._idle_rebuild_unsub = async_call_later(
            self.hass, idle_timeout, self._handle_idle_rebuild
        )

    async def _handle_idle_rebuild(self, _now) -> None:
        """Baut nach Ablauf der Idle-Zeit einmalig proaktiv eine frische Sitzung auf.

        Stellt sich NICHT selbst erneut - ohne eine weitere echte Nachricht bleibt
        der Timer danach aus, es wird also nicht endlos alle CONF_IDLE_TIMEOUT
        Sekunden neu verbunden.
        """
        self._idle_rebuild_unsub = None
        idle_timeout = self._data.get(CONF_IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT)

        async with self._lock:
            # Race-Schutz: zwischen Timer-Ablauf und Lock-Erhalt koennte eine echte
            # Anfrage reingekommen sein, die _last_activity schon aktualisiert und
            # dabei bereits ihren eigenen Timer gestellt hat - dann hier nachziehen
            # statt den frischeren Timer dieser Anfrage zu verwerfen.
            if (time.monotonic() - self._last_activity) < idle_timeout:
                self._schedule_idle_rebuild()
                return

            _LOGGER.info(
                "Proaktiver Sitzungsneuaufbau nach %s Sekunden Inaktivitaet.",
                idle_timeout,
            )
            try:
                await self._teardown()
                await self._ensure_session()
            except (asyncssh.Error, OSError) as err:
                _LOGGER.warning(
                    "Proaktiver Sitzungsneuaufbau fehlgeschlagen (%s).", err
                )

    async def async_process(
        self, user_input: conversation.ConversationInput
    ) -> conversation.ConversationResult:
        """Verarbeitet eine Sprachanfrage."""
        text = (user_input.text or "").strip()

        async with self._lock:
            if text.lower() in self._abort_phrases:
                reply_text = await self._handle_abort()
            else:
                await self._ensure_session()
                try:
                    reply_text = await self._send_query(text)
                except (asyncssh.Error, OSError, asyncio.TimeoutError) as err:
                    _LOGGER.warning(
                        "Verbindung zur Claude-Code-Maschine verloren (%s) - "
                        "naechste Anfrage startet neu, kein Recovery-Versuch.",
                        err,
                    )
                    await self._teardown()
                    reply_text = (
                        "Die Verbindung ist abgebrochen. Bitte gleich noch einmal fragen."
                    )

            self._last_activity = time.monotonic()

        # Timer startet bzw. startet neu - nur hier, bei einer echten Nachricht.
        self._schedule_idle_rebuild()

        response = ha_intent.IntentResponse(language=user_input.language)
        response.async_set_speech(reply_text)
        return conversation.ConversationResult(
            response=response, conversation_id=user_input.conversation_id
        )

    async def _ensure_session(self) -> None:
        """Baut die Sitzung neu auf, wenn sie fehlt oder die Idle-Zeit ueberschritten ist.

        Dies ist das Sicherheitsnetz fuer den Fall, dass der proaktive Timer aus
        irgendeinem Grund nicht gefeuert hat (z.B. HA war zwischenzeitlich down).
        Im Normalfall hat der proaktive Rebuild schon laengst eine warme Sitzung parat.
        """
        idle_timeout = self._data.get(CONF_IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT)
        idle_expired = bool(
            self._last_activity
            and (time.monotonic() - self._last_activity) > idle_timeout
        )

        if self._process is not None and not idle_expired:
            return

        await self._teardown()

        self._ssh_conn = await asyncssh.connect(
            self._data[CONF_SSH_HOST],
            port=self._data.get(CONF_SSH_PORT, 22),
            username=self._data[CONF_SSH_USERNAME],
            client_keys=[self._data[CONF_SSH_KEY_PATH]],
            known_hosts=None,  # TODO: known_hosts-Pruefung ergaenzen statt zu deaktivieren
        )

        command = [self._data.get(CONF_CLAUDE_BINARY, DEFAULT_CLAUDE_BINARY)]
        command += [
            "-p",
            "--verbose",
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--permission-mode", "bypassPermissions",
            "--model", "claude-haiku-4-5-20251001",
        ]
        self._process = await self._ssh_conn.create_process(" ".join(command))

    async def _teardown(self) -> None:
        """Beendet Prozess und Verbindung, ohne Fehler weiterzureichen (kein Recovery-Versuch)."""
        for closer in (self._process, self._ssh_conn):
            if closer is not None:
                try:
                    closer.close()
                except Exception:  # pylint: disable=broad-except
                    pass
        self._process = None
        self._ssh_conn = None

    async def _send_query(self, text: str) -> str:
        """Schickt eine Nutzeranfrage als JSON-Zeile, sammelt Text bis 'result'."""
        assert self._process is not None

        message = {"type": "user", "message": {"role": "user", "content": text}}
        self._process.stdin.write(json.dumps(message) + "\n")

        collected_text: list[str] = []
        while True:
            line = await asyncio.wait_for(self._process.stdout.readline(), timeout=90)
            if not line:
                raise OSError("Claude-Code-Prozess hat stdout geschlossen")

            event = json.loads(line)
            event_type = event.get("type")

            if event_type == "assistant":
                for block in event.get("message", {}).get("content", []):
                    if block.get("type") == "text":
                        collected_text.append(block["text"])
            elif event_type == "result":
                break

        return "".join(collected_text) or "Ich habe dazu keine Antwort bekommen."

    async def _pause_all_media(self) -> None:
        """Pausiert alle Medienwiedergaben im Haus (entity_id: all), Fehler werden nur geloggt."""
        try:
            await self.hass.services.async_call(
                "media_player",
                "media_pause",
                {"entity_id": "all"},
                blocking=False,
            )
        except Exception as err:  # pylint: disable=broad-except
            _LOGGER.warning("Konnte Medienwiedergaben beim Abbruch nicht pausieren (%s).", err)

    async def _handle_abort(self) -> str:
        """Pausiert alle Medienwiedergaben, schickt ein Interrupt-Kommando, killt notfalls hart."""
        await self._pause_all_media()

        if self._process is None:
            return "Medienwiedergabe pausiert. Es lief sonst nichts, das ich abbrechen koennte."

        request_id = str(uuid.uuid4())
        control_message = {
            "type": "control_request",
            "request_id": request_id,
            "request": {"subtype": "interrupt"},
        }
        try:
            self._process.stdin.write(json.dumps(control_message) + "\n")
            await asyncio.wait_for(
                self._wait_for_control_response(request_id), INTERRUPT_GRACE_PERIOD
            )
            return "Abgebrochen und Medienwiedergabe pausiert."
        except (asyncio.TimeoutError, asyncssh.Error, OSError):
            _LOGGER.info(
                "Interrupt hat innerhalb von %.1fs nicht reagiert, Prozess wird hart beendet.",
                INTERRUPT_GRACE_PERIOD,
            )
            await self._teardown()
            return (
                "Abgebrochen und Medienwiedergabe pausiert "
                "(Sitzung wird beim naechsten Mal neu aufgebaut)."
            )

    async def _wait_for_control_response(self, request_id: str) -> None:
        """Wartet auf die control_response zum gegebenen request_id."""
        assert self._process is not None
        while True:
            line = await self._process.stdout.readline()
            if not line:
                raise OSError("Claude-Code-Prozess hat stdout geschlossen")
            event = json.loads(line)
            if (
                event.get("type") == "control_response"
                and event.get("response", {}).get("request_id") == request_id
            ):
                return
