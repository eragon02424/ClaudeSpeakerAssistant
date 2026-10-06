"""Conversation-Plattform fuer Claude Speaker Assistant (Version 0.3.0).

Architektur (siehe Second-Brain-Projekt "claude-smart-home-zentrale"):
- Claude Code oeffnet selbst keinen Netzwerk-Port; die SSH-Verbindung uebernimmt den Transport.
- Ein einzelner Claude-Code-Prozess pro Sitzung laeuft im Streaming-JSON-Modus und bleibt
  zwischen mehreren Sprachanfragen offen (kein Neustart pro Anfrage).

Neu in Version 0.3.0 (Wuensche von Jonathan, 2026-10-06):

1. Aufbau im Hintergrund, alte Sitzung laeuft weiter (Idle-Rebuild)
   Nach CONF_IDLE_TIMEOUT Sekunden ohne echte Anfrage wird eine ERSATZ-Sitzung im Hintergrund
   aufgebaut (SSH, Claude Code, alle MCP-Server dauern bis ca. 90 s). Die alte Sitzung bleibt
   dabei aktiv und beantwortet Anfragen. Erst wenn die neue Sitzung die Bereitschaftsfrage
   (READY_PROMPT) beantwortet hat, wird umgeschaltet und die alte beendet.
   Wichtige Regel: Kommt waehrend dieses Aufbaus eine echte Anfrage (auch ein Abbruch-Wort),
   wird die Ersatz-Sitzung VERWORFEN und das Gespraech in der alten Sitzung weitergefuehrt;
   der Idle-Timer startet dadurch neu (CONF_IDLE_TIMEOUT ab dem Ende dieser Anfrage). So geht
   der Kontext mitten in einem Gespraech nie verloren. Nach dem Umschalten startet die neue
   Sitzung ohne Kontext (wie bisher beim Idle-Rebuild).
   Der Idle-Timer laeuft weiterhin NICHT endlos: Er wird nur von echten Anfragen gestartet.

2. Sofortiger Wiederaufbau bei Verbindungsabbruch
   Geht die Verbindung verloren (Anfrage schlaegt fehl, Heartbeat schlaegt fehl, Prozess oder
   SSH-Verbindung enden unerwartet - erkannt durch einen Beobachter pro Sitzung, SSH-Keepalives
   erkennen auch stille Abbrueche nach ca. 90 s, Abbruch-Wort bleibt ohne Antwort), wird sofort
   im Hintergrund eine neue Sitzung aufgebaut und nicht erst bei der naechsten Anfrage. Kommt
   waehrend dieses Wiederaufbaus eine Anfrage, wartet sie (ohne die Sperre zu halten) bis zu
   RECOVERY_WAIT_TIMEOUT Sekunden auf die neue Sitzung. Schlaegt der Wiederaufbau fehl (z.B.
   VM aus), wird mit wachsendem Abstand (RETRY_DELAYS) wiederholt; eine echte Anfrage loest
   zusaetzlich sofort einen Versuch aus.

3. Warmstart beim HA-Start blockiert nicht mehr: Der Aufbau laeuft als Hintergrundaufgabe.

Bestehende Entscheidungen:
- Heartbeat (seit 2026-10-03): alle CONF_HEARTBEAT_INTERVAL Sekunden eine kurze Ja/Nein-Frage
  durch die aktive Sitzung, rein zur Kontrolle, ob SSH/Sitzung noch lebt. Ruehrt WEDER
  _last_activity NOCH den Idle-Timer an (zaehlt nicht als echte Anfrage) und wird
  uebersprungen, wenn gerade erst eine echte Anfrage war. Neu: Bei Fehlschlag wird sofort
  neu aufgebaut (siehe Punkt 2), ohne aktive Sitzung wird ein Aufbau gestartet.
- Bewusst immer der volle MCP-Serverausbau (keine --mcp-config-Reduktion, Jonathan 2026-10-03:
  Server sollen zentral ueber den claude.ai-Account verwaltet werden). Dabei gefundener
  Flag-Reihenfolge-Bug: --mcp-config ist variadic und verschluckt ein direkt danach stehendes
  --strict-mcp-config als Dateipfad.
- Abbruch laeuft ueber das control_request/interrupt-Kommando des Streaming-JSON-Protokolls
  mit hartem Kill als Sicherheitsnetz; pausiert seit 2026-10-02 zusaetzlich ALLE
  Medienwiedergaben. Bewusst kein lokaler HA-Intent (wuerde wegen prefer_local_intents den
  Interrupt verhindern).
- --permission-mode bypassPermissions (2026-10-02): im headless Modus gibt es keine
  interaktive Rueckfrage, ohne den Flag wuerde MCP-Zugriff verweigert.
- --model claude-haiku-4-5-20251001 ist testweise gesetzt (2026-10-02).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

import asyncssh

from homeassistant.components import conversation
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import MATCH_ALL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers import intent as ha_intent
from homeassistant.helpers.event import async_call_later, async_track_time_interval

from .const import (
    BUILD_READY_TIMEOUT,
    CONF_ABORT_PHRASES,
    CONF_CLAUDE_BINARY,
    CONF_HEARTBEAT_INTERVAL,
    CONF_IDLE_TIMEOUT,
    CONF_SSH_HOST,
    CONF_SSH_KEY_PATH,
    CONF_SSH_PORT,
    CONF_SSH_USERNAME,
    DEFAULT_ABORT_PHRASES,
    DEFAULT_CLAUDE_BINARY,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_IDLE_TIMEOUT,
    DOMAIN,
    HEARTBEAT_PROMPT,
    INTERRUPT_GRACE_PERIOD,
    READY_PROMPT,
    RECOVERY_WAIT_TIMEOUT,
    REQUEST_LINE_TIMEOUT,
    RETRY_DELAYS,
    SSH_CONNECT_TIMEOUT,
    SSH_KEEPALIVE_COUNT_MAX,
    SSH_KEEPALIVE_INTERVAL,
)

_LOGGER = logging.getLogger(__name__)


@dataclass
class ClaudeSession:
    """Eine SSH-Verbindung mit dem dazugehoerigen Claude-Code-Prozess."""

    conn: asyncssh.SSHClientConnection
    process: asyncssh.SSHClientProcess
    # Wird vor absichtlichem Schliessen gesetzt, damit der Beobachter kein Abbruch meldet.
    closing: bool = False
    watcher: asyncio.Task | None = None


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

        # Aktive Sitzung (beantwortet Anfragen) - None, solange keine bereit ist.
        self._session: ClaudeSession | None = None
        # Sperre: Anfragen und Heartbeat laufen nacheinander durch die aktive Sitzung.
        self._lock = asyncio.Lock()
        self._last_activity: float = 0.0
        self._idle_rebuild_unsub: Callable[[], None] | None = None
        self._heartbeat_unsub: Callable[[], None] | None = None
        self._retry_unsub: Callable[[], None] | None = None

        # Hintergrund-Aufbau einer (Ersatz-)Sitzung
        self._build_task: asyncio.Task | None = None
        # True = es gibt eine aktive Sitzung, die Ersatz wird nur vorbereitet (darf von einer
        # neuen Anfrage verworfen werden); False = es gibt keine aktive Sitzung (Wiederaufbau).
        self._build_replaces_active: bool = False
        # Wird beim Verwerfen erhoeht, damit ein schon fertiger Aufbau nicht mehr umschaltet.
        self._build_generation: int = 0
        self._build_failures: int = 0

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

    # ------------------------------------------------------------------
    # Lebenszyklus
    # ------------------------------------------------------------------

    async def async_added_to_hass(self) -> None:
        """Startet beim Start von Home Assistant den Aufbau einer ersten Sitzung im Hintergrund.

        Blockiert den Start nicht. Startet bewusst NICHT den Idle-Timer - der beginnt erst mit
        der ersten echten Sprachnachricht (siehe async_process). Startet DAGEGEN sofort den
        Heartbeat-Timer.
        """
        await super().async_added_to_hass()
        self._start_build(replace_active=False, reason="HA-Start")

        heartbeat_interval = self._data.get(
            CONF_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL
        )
        self._heartbeat_unsub = async_track_time_interval(
            self.hass, self._handle_heartbeat, timedelta(seconds=heartbeat_interval)
        )

    async def async_will_remove_from_hass(self) -> None:
        """Raeumt Timer, Hintergrundaufbau und Sitzung auf, wenn die Entity entfernt wird."""
        if self._idle_rebuild_unsub is not None:
            self._idle_rebuild_unsub()
            self._idle_rebuild_unsub = None
        if self._heartbeat_unsub is not None:
            self._heartbeat_unsub()
            self._heartbeat_unsub = None
        if self._retry_unsub is not None:
            self._retry_unsub()
            self._retry_unsub = None

        self._build_generation += 1
        if self._build_task is not None and not self._build_task.done():
            self._build_task.cancel()

        session = self._session
        self._session = None
        if session is not None:
            self._close_session(session)

    # ------------------------------------------------------------------
    # Idle-Timer und Ersatz-Aufbau
    # ------------------------------------------------------------------

    def _schedule_idle_rebuild(self) -> None:
        """Plant den Ersatz-Aufbau CONF_IDLE_TIMEOUT Sekunden ab jetzt.

        Wird NUR von einer echten Sprachnachricht (async_process) aufgerufen, nie
        automatisch verkettet und NIE vom Heartbeat-Check.
        """
        if self._idle_rebuild_unsub is not None:
            self._idle_rebuild_unsub()
        idle_timeout = self._data.get(CONF_IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT)
        self._idle_rebuild_unsub = async_call_later(
            self.hass, idle_timeout, self._handle_idle_rebuild
        )

    async def _handle_idle_rebuild(self, _now) -> None:
        """Startet nach Ablauf der Idle-Zeit einmalig den Aufbau einer Ersatz-Sitzung.

        Die alte Sitzung bleibt aktiv, bis die neue bereit ist. Stellt sich NICHT selbst
        erneut - ohne weitere echte Nachricht bleibt der Timer danach aus.
        """
        self._idle_rebuild_unsub = None
        idle_timeout = self._data.get(CONF_IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT)

        # Race-Schutz: Kam zwischen Timer-Ablauf und hier eine echte Anfrage, hat diese ihren
        # eigenen Timer gestellt - dann hier nachziehen statt den frischeren zu verwerfen.
        if (time.monotonic() - self._last_activity) < idle_timeout:
            self._schedule_idle_rebuild()
            return

        if self._session is None:
            self._start_build(replace_active=False, reason="Idle-Ablauf ohne aktive Sitzung")
            return

        _LOGGER.info(
            "Ersatz-Sitzung wird nach %s Sekunden Inaktivitaet im Hintergrund aufgebaut, "
            "die alte laeuft bis zur Bereitschaft weiter.",
            idle_timeout,
        )
        self._start_build(replace_active=True, reason="Idle-Ablauf")

    def _cancel_replacement_build(self) -> None:
        """Verwirft einen laufenden ERSATZ-Aufbau (nie einen Wiederaufbau ohne aktive Sitzung)."""
        if (
            self._build_task is not None
            and not self._build_task.done()
            and self._build_replaces_active
        ):
            self._build_generation += 1
            self._build_task.cancel()
            _LOGGER.info(
                "Neue Anfrage waehrend des Ersatz-Aufbaus: neue Sitzung wird verworfen, "
                "das Gespraech laeuft in der alten Sitzung weiter."
            )

    # ------------------------------------------------------------------
    # Aufbau im Hintergrund
    # ------------------------------------------------------------------

    def _start_build(self, replace_active: bool, reason: str) -> None:
        """Startet den Aufbau einer neuen Sitzung im Hintergrund (nichts, wenn schon einer laeuft)."""
        if self._build_task is not None and not self._build_task.done():
            return
        if self._retry_unsub is not None:
            self._retry_unsub()
            self._retry_unsub = None

        self._build_replaces_active = replace_active
        self._build_task = self.hass.async_create_background_task(
            self._build_session(reason, self._build_generation),
            name=f"{DOMAIN}_build",
        )

    async def _build_session(self, reason: str, generation: int) -> None:
        """Baut eine Sitzung auf, prueft sie auf Bereitschaft und schaltet erst dann um."""
        candidate: ClaudeSession | None = None
        started = time.monotonic()
        _LOGGER.info("Aufbau einer neuen Claude-Code-Sitzung gestartet (%s).", reason)
        try:
            candidate = await self._open_session()
            reply = await self._query_session(candidate, READY_PROMPT, BUILD_READY_TIMEOUT)

            # Umschalten: im Lock-Block gibt es KEINEN await, damit ein Abbruch durch eine
            # neue Anfrage nicht mitten im Umschalten dazwischenfunkt.
            async with self._lock:
                if generation != self._build_generation:
                    _LOGGER.info("Neue Sitzung war bereit, wurde aber bereits verworfen.")
                    return
                old = self._session
                self._session = candidate
                self._watch_session(candidate)
                candidate = None
                if old is not None:
                    self._close_session(old)

            self._build_failures = 0
            _LOGGER.info(
                "Neue Claude-Code-Sitzung bereit nach %.0f s (%s), umgeschaltet. "
                "Antwort auf die Bereitschaftsfrage: %s",
                time.monotonic() - started,
                reason,
                reply.strip(),
            )
        except asyncio.CancelledError:
            _LOGGER.info("Aufbau der neuen Claude-Code-Sitzung abgebrochen (%s).", reason)
            raise
        except (asyncssh.Error, OSError, asyncio.TimeoutError, ValueError) as err:
            if self._session is None:
                # Keine aktive Sitzung: weiter versuchen, mit wachsendem Abstand.
                self._build_failures += 1
                delay = RETRY_DELAYS[min(self._build_failures, len(RETRY_DELAYS)) - 1]
                _LOGGER.warning(
                    "Aufbau der Claude-Code-Sitzung fehlgeschlagen (%s: %s), Versuch %d, "
                    "naechster Versuch in %d s.",
                    type(err).__name__,
                    err,
                    self._build_failures,
                    delay,
                )
                self._retry_unsub = async_call_later(self.hass, delay, self._handle_retry)
            else:
                _LOGGER.warning(
                    "Aufbau der Ersatz-Sitzung fehlgeschlagen (%s: %s). "
                    "Die alte Sitzung bleibt aktiv.",
                    type(err).__name__,
                    err,
                )
        finally:
            if candidate is not None:
                self._close_session(candidate)

    async def _handle_retry(self, _now) -> None:
        """Wiederholt einen fehlgeschlagenen Wiederaufbau."""
        self._retry_unsub = None
        if self._session is None:
            self._start_build(replace_active=False, reason="Wiederholung")

    async def _open_session(self) -> ClaudeSession:
        """Oeffnet SSH-Verbindung und startet den Claude-Code-Prozess (noch ohne Bereitschaftspruefung)."""
        conn = await asyncio.wait_for(
            asyncssh.connect(
                self._data[CONF_SSH_HOST],
                port=self._data.get(CONF_SSH_PORT, 22),
                username=self._data[CONF_SSH_USERNAME],
                client_keys=[self._data[CONF_SSH_KEY_PATH]],
                known_hosts=None,  # TODO: known_hosts-Pruefung ergaenzen statt zu deaktivieren
                keepalive_interval=SSH_KEEPALIVE_INTERVAL,
                keepalive_count_max=SSH_KEEPALIVE_COUNT_MAX,
            ),
            timeout=SSH_CONNECT_TIMEOUT,
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
        try:
            process = await conn.create_process(" ".join(command))
        except BaseException:
            conn.close()
            raise
        return ClaudeSession(conn=conn, process=process)

    # ------------------------------------------------------------------
    # Beobachtung und Verlust der aktiven Sitzung
    # ------------------------------------------------------------------

    def _watch_session(self, session: ClaudeSession) -> None:
        """Startet einen Beobachter, der ein unerwartetes Ende der Sitzung sofort meldet."""
        session.watcher = self.hass.async_create_background_task(
            self._watch(session), name=f"{DOMAIN}_watch"
        )

    async def _watch(self, session: ClaudeSession) -> None:
        try:
            await session.process.wait_closed()
        except asyncio.CancelledError:
            raise
        except Exception:  # pylint: disable=broad-except
            pass
        if session.closing or self._session is not session:
            return
        _LOGGER.warning(
            "Claude-Code-Prozess bzw. SSH-Verbindung wurde unerwartet beendet - "
            "Wiederaufbau startet sofort."
        )
        self._handle_session_lost("unerwartetes Ende")

    def _handle_session_lost(self, reason: str) -> None:
        """Schliesst die aktive Sitzung und startet SOFORT den Wiederaufbau (kein Warten auf die naechste Anfrage).

        Synchron und ohne await, darf daher auch innerhalb der Sperre aufgerufen werden.
        """
        session = self._session
        self._session = None
        if session is not None:
            self._close_session(session)

        if self._build_task is not None and not self._build_task.done():
            # Ein Aufbau laeuft schon (z.B. Ersatz-Aufbau): er wird zur neuen aktiven Sitzung
            # und darf deshalb nicht mehr von einer neuen Anfrage verworfen werden.
            self._build_replaces_active = False
        else:
            self._start_build(replace_active=False, reason=f"Wiederaufbau ({reason})")

    @staticmethod
    def _close_session(session: ClaudeSession) -> None:
        """Beendet Prozess und Verbindung ohne Fehler weiterzureichen. Synchron, ohne await."""
        session.closing = True
        if session.watcher is not None and session.watcher is not asyncio.current_task():
            session.watcher.cancel()
        for closer in (session.process, session.conn):
            try:
                closer.close()
            except Exception:  # pylint: disable=broad-except
                pass

    async def _wait_for_session(self) -> None:
        """Wartet OHNE die Sperre zu halten, bis eine im Aufbau befindliche Sitzung bereit ist.

        Ohne Sperre, weil das Umschalten der fertigen Sitzung die Sperre braucht.
        """
        if self._session is not None:
            return
        if self._build_task is None or self._build_task.done():
            self._start_build(replace_active=False, reason="Anfrage ohne aktive Sitzung")
        task = self._build_task
        if task is not None:
            # wait statt wait_for: bricht die Anfrage ab, laeuft der Aufbau trotzdem weiter.
            await asyncio.wait({task}, timeout=RECOVERY_WAIT_TIMEOUT)

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def _handle_heartbeat(self, _now) -> None:
        """Periodischer Verbindungscheck (alle CONF_HEARTBEAT_INTERVAL Sekunden).

        Prueft per kurzer Ja/Nein-Frage durch die aktive Sitzung, ob SSH-Verbindung und
        Claude-Code-Prozess noch leben. Ruehrt WEDER _last_activity NOCH den Idle-Timer an.
        Wird uebersprungen, wenn gerade erst eine echte Anfrage war.

        Fehlschlag: sofortiger Wiederaufbau. Keine aktive Sitzung: Aufbau wird gestartet
        (falls nicht ohnehin einer laeuft).
        """
        heartbeat_interval = self._data.get(
            CONF_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL
        )
        if self._last_activity and (
            time.monotonic() - self._last_activity
        ) < heartbeat_interval:
            _LOGGER.debug(
                "Heartbeat-Check uebersprungen - gerade erst eine echte Anfrage gehabt."
            )
            return

        if self._session is None:
            _LOGGER.info(
                "Heartbeat-Check: keine aktive Sitzung vorhanden, Aufbau laeuft bzw. wird gestartet."
            )
            self._start_build(replace_active=False, reason="Heartbeat ohne Sitzung")
            return

        async with self._lock:
            session = self._session
            if session is None:
                return
            try:
                reply = await self._query_session(session, HEARTBEAT_PROMPT, REQUEST_LINE_TIMEOUT)
                _LOGGER.info(
                    "Heartbeat-Check: SSH-Verbindung lebt, Claude antwortete: %s",
                    reply.strip(),
                )
            except (asyncssh.Error, OSError, asyncio.TimeoutError, ValueError) as err:
                _LOGGER.warning(
                    "Heartbeat-Check: SSH-Verbindung/Sitzung wirkt tot (%s) - "
                    "Wiederaufbau startet sofort.",
                    err,
                )
                if self._session is session:
                    self._handle_session_lost("Heartbeat fehlgeschlagen")

    # ------------------------------------------------------------------
    # Echte Anfragen
    # ------------------------------------------------------------------

    async def async_process(
        self, user_input: conversation.ConversationInput
    ) -> conversation.ConversationResult:
        """Verarbeitet eine Sprachanfrage."""
        text = (user_input.text or "").strip()
        is_abort = text.lower() in self._abort_phrases

        # Echte Anfrage: Laeuft gerade ein Ersatz-Aufbau, wird er verworfen und in der alten
        # Sitzung weitergearbeitet (Kontext bleibt erhalten, Idle-Timer startet unten neu).
        self._cancel_replacement_build()

        # Ohne aktive Sitzung auf den laufenden Wiederaufbau warten - ohne Sperre.
        if not is_abort and self._session is None:
            await self._wait_for_session()

        async with self._lock:
            if is_abort:
                reply_text = await self._handle_abort()
            elif self._session is None:
                reply_text = (
                    "Die Verbindung zum Assistenten wird gerade neu aufgebaut. "
                    "Bitte gleich noch einmal fragen."
                )
            else:
                session = self._session
                try:
                    reply_text = await self._query_session(session, text, REQUEST_LINE_TIMEOUT)
                except (asyncssh.Error, OSError, asyncio.TimeoutError, ValueError) as err:
                    _LOGGER.warning(
                        "Verbindung zur Claude-Code-Maschine verloren (%s) - "
                        "Wiederaufbau startet sofort.",
                        err,
                    )
                    if self._session is session:
                        self._handle_session_lost("Anfrage fehlgeschlagen")
                    reply_text = (
                        "Die Verbindung ist abgebrochen. Ich baue sie neu auf, "
                        "bitte gleich noch einmal fragen."
                    )

            self._last_activity = time.monotonic()

        # Timer startet bzw. startet neu - nur hier, bei einer echten Nachricht.
        self._schedule_idle_rebuild()

        response = ha_intent.IntentResponse(language=user_input.language)
        response.async_set_speech(reply_text)
        return conversation.ConversationResult(
            response=response, conversation_id=user_input.conversation_id
        )

    async def _query_session(self, session: ClaudeSession, text: str, timeout: float) -> str:
        """Schickt eine Anfrage als JSON-Zeile an die Sitzung, sammelt Text bis 'result'.

        timeout gilt je gelesener Ausgabezeile.
        """
        message = {"type": "user", "message": {"role": "user", "content": text}}
        session.process.stdin.write(json.dumps(message) + "\n")

        collected_text: list[str] = []
        while True:
            line = await asyncio.wait_for(session.process.stdout.readline(), timeout=timeout)
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

    # ------------------------------------------------------------------
    # Abbruch
    # ------------------------------------------------------------------

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

        session = self._session
        if session is None:
            return "Medienwiedergabe pausiert. Es lief sonst nichts, das ich abbrechen koennte."

        request_id = str(uuid.uuid4())
        control_message = {
            "type": "control_request",
            "request_id": request_id,
            "request": {"subtype": "interrupt"},
        }
        try:
            session.process.stdin.write(json.dumps(control_message) + "\n")
            await asyncio.wait_for(
                self._wait_for_control_response(session, request_id), INTERRUPT_GRACE_PERIOD
            )
            return "Abgebrochen und Medienwiedergabe pausiert."
        except (asyncio.TimeoutError, asyncssh.Error, OSError, ValueError):
            _LOGGER.info(
                "Interrupt hat innerhalb von %.1fs nicht reagiert, Prozess wird hart beendet "
                "und sofort neu aufgebaut.",
                INTERRUPT_GRACE_PERIOD,
            )
            if self._session is session:
                self._handle_session_lost("Abbruch ohne Antwort")
            return (
                "Abgebrochen und Medienwiedergabe pausiert "
                "(die Sitzung wird neu aufgebaut)."
            )

    async def _wait_for_control_response(self, session: ClaudeSession, request_id: str) -> None:
        """Wartet auf die control_response zum gegebenen request_id."""
        while True:
            line = await session.process.stdout.readline()
            if not line:
                raise OSError("Claude-Code-Prozess hat stdout geschlossen")
            event = json.loads(line)
            if (
                event.get("type") == "control_response"
                and event.get("response", {}).get("request_id") == request_id
            ):
                return
