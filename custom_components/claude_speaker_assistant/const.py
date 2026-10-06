"""Konstanten fuer Claude Speaker Assistant (Version 0.3.0)."""

DOMAIN = "claude_speaker_assistant"

CONF_SSH_HOST = "ssh_host"
CONF_SSH_PORT = "ssh_port"
CONF_SSH_USERNAME = "ssh_username"
CONF_SSH_KEY_PATH = "ssh_key_path"
CONF_CLAUDE_BINARY = "claude_binary"
CONF_IDLE_TIMEOUT = "idle_timeout"
CONF_ABORT_PHRASES = "abort_phrases"
CONF_HEARTBEAT_INTERVAL = "heartbeat_interval"

DEFAULT_SSH_PORT = 22
DEFAULT_CLAUDE_BINARY = "claude"
# 10 Minuten - siehe Second-Brain-Projekt "claude-smart-home-zentrale", Entscheidung vom 2026-09-29.
DEFAULT_IDLE_TIMEOUT = 600
# "beenden" am 2026-10-02 ergaenzt (Second-Brain-Projekt "claude-smart-home-zentrale"):
# Abbruch-Woerter pausieren jetzt zusaetzlich alle Medienwiedergaben, siehe _handle_abort.
DEFAULT_ABORT_PHRASES = ["abbruch", "abbrechen", "stopp", "stop", "beenden"]

# Wartezeit nach einem gesendeten Interrupt, bevor hart gekillt wird
# (Entscheidung: Interrupt zuerst versuchen, Kill als Sicherheitsnetz).
INTERRUPT_GRACE_PERIOD = 2.0

# 5 Minuten - periodischer "bist du noch da"-Check der SSH-Verbindung/Sitzung, komplett
# unabhaengig vom 10-Minuten-Idle-Rebuild (CONF_IDLE_TIMEOUT) - zaehlt bewusst nicht als
# echte Anfrage, siehe _handle_heartbeat.
DEFAULT_HEARTBEAT_INTERVAL = 300
HEARTBEAT_PROMPT = "Bist du noch da? Antworte nur mit ja oder nein."

# --- Neu in Version 0.3.0 (Jonathan, 2026-10-06): Aufbau im Hintergrund ---

# Kurze Frage, mit der eine neu aufgebaute Sitzung auf Bereitschaft geprueft wird. Erst die
# Antwort beweist, dass Claude Code UND alle MCP-Server geladen sind (im Streaming-JSON-Modus
# kommt vorher keine Ausgabe).
READY_PROMPT = HEARTBEAT_PROMPT

# Maximale Zeit fuer den Aufbau einer neuen Sitzung (SSH + Claude Code + MCP-Server + Bereitschaftsfrage).
# Gemessen am 2026-10-06: 2 bis 6 s. Das Limit ist ein grosszuegiges Sicherheitsnetz.
BUILD_READY_TIMEOUT = 240

# Maximale Zeit, die eine echte Anfrage auf eine gerade im Aufbau befindliche Sitzung wartet,
# wenn es keine aktive Sitzung gibt (z.B. direkt nach einem Verbindungsabbruch).
RECOVERY_WAIT_TIMEOUT = 120

# Wartezeit je Zeile der Claude-Code-Ausgabe bei normalen Anfragen (wie bisher: 90 s).
REQUEST_LINE_TIMEOUT = 90

# Zeitlimit fuer den SSH-Verbindungsaufbau (sonst wartet asyncssh bis zum TCP-Timeout).
# Umgesetzt per asyncio.wait_for, unabhaengig von der asyncssh-Version.
SSH_CONNECT_TIMEOUT = 15

# SSH-Keepalives: Reagiert die Gegenseite SSH_KEEPALIVE_COUNT_MAX Mal im Abstand von
# SSH_KEEPALIVE_INTERVAL Sekunden nicht (z.B. VM eingeschlafen), schliesst asyncssh die
# Verbindung - der Beobachter erkennt das und startet den Wiederaufbau (nach ca. 90 s),
# statt erst beim naechsten Heartbeat oder bei der naechsten Anfrage.
SSH_KEEPALIVE_INTERVAL = 30
SSH_KEEPALIVE_COUNT_MAX = 3

# Wartezeiten (Sekunden) zwischen Wiederholungen, wenn der Wiederaufbau OHNE aktive Sitzung
# fehlschlaegt (z.B. VM aus). Der letzte Wert gilt danach dauerhaft.
RETRY_DELAYS = (15, 30, 60, 120, 300)
