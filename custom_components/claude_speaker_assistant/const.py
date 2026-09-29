"""Konstanten fuer Claude Speaker Assistant."""

DOMAIN = "claude_speaker_assistant"

CONF_SSH_HOST = "ssh_host"
CONF_SSH_PORT = "ssh_port"
CONF_SSH_USERNAME = "ssh_username"
CONF_SSH_KEY_PATH = "ssh_key_path"
CONF_CLAUDE_BINARY = "claude_binary"
CONF_MCP_CONFIG_PATH = "mcp_config_path"
CONF_IDLE_TIMEOUT = "idle_timeout"
CONF_ABORT_PHRASES = "abort_phrases"

DEFAULT_SSH_PORT = 22
DEFAULT_CLAUDE_BINARY = "claude"
# 10 Minuten - siehe Second-Brain-Projekt "claude-smart-home-zentrale", Entscheidung vom 2026-09-29.
DEFAULT_IDLE_TIMEOUT = 600
DEFAULT_ABORT_PHRASES = ["abbruch", "abbrechen", "stopp", "stop"]

# Wartezeit nach einem gesendeten Interrupt, bevor hart gekillt wird
# (Entscheidung: Interrupt zuerst versuchen, Kill als Sicherheitsnetz).
INTERRUPT_GRACE_PERIOD = 2.0
