# ClaudeSpeakerAssistant

Home Assistant Custom Integration: Claude Code als Sprachassistent-Conversation-Agent.

## Idee

Diese Integration registriert einen Home-Assistant-Conversation-Agent, der Sprachanfragen aus der Assist-Pipeline nicht an den Standard-Agenten schickt, sondern an eine Claude-Code-Instanz, die auf einer separaten Maschine (z. B. einer Linux-VM) läuft. Die Verbindung läuft über SSH; Claude Code selbst öffnet keinen Netzwerk-Port.

## Architektur (Kurzfassung)

- Ein Claude-Code-Prozess läuft dauerhaft im Streaming-JSON-Modus (`--input-format stream-json --output-format stream-json`) auf der Zielmaschine.
- Diese Integration hält eine SSH-Verbindung zu dieser Maschine offen und schickt/liest JSON-Lines über diesen Kanal.
- Nach 10 Minuten ohne Sprachanfrage wird die Sitzung einmalig proaktiv neu aufgebaut (kein Gesprächskontext über die Pause hinweg nötig).
- Abbruch einer laufenden Antwort läuft über das offiziell dokumentierte `control_request`/`interrupt`-Kommando des Streaming-JSON-Protokolls (kein Agent SDK nötig), mit hartem Prozess-Kill als Sicherheitsnetz.
- Kein separates Wrapper-Skript auf der Zielmaschine nötig – nur ein laufender SSH-Server und installiertes Claude Code dort.

## Status

Frühe Entwicklungsphase. Details und Entscheidungsverlauf stehen im privaten Second-Brain-Projekt "Claude als Sprachassistent und Smart-Home-Zentrale".

## Installation (HACS)

1. In HACS: Benutzerdefiniertes Repository hinzufügen, Kategorie „Integration".
2. `Claude Speaker Assistant` installieren.
3. Home Assistant neu starten.
4. Einstellungen → Geräte & Dienste → Integration hinzufügen → „Claude Speaker Assistant".
5. Unter Einstellungen → Sprachassistenten den neuen Conversation Agent auswählen.
