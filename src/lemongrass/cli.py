import importlib
import sys

_COMMANDS = {
    "laps": "lemongrass.laps",
    "race-backfill": "lemongrass.race_backfill",
    "races": "lemongrass.races",
    "telem": "lemongrass.telem",
    "pisugar-monitor": "lemongrass.pisugar_monitor",
    "race-diagnose": "lemongrass.race_diagnose",
}


def main():
    if len(sys.argv) >= 2 and sys.argv[1] in ("-h", "--help"):
        print("Usage: lemongrass <command> [args]")
        print(f"Commands: {', '.join(_COMMANDS)}")
        sys.exit(0)

    if len(sys.argv) < 2 or sys.argv[1] not in _COMMANDS:
        print("Usage: lemongrass <command> [args]")
        print(f"Commands: {', '.join(_COMMANDS)}")
        sys.exit(1)

    cmd = sys.argv.pop(1)
    sys.argv[0] = f"lemongrass-{cmd}"
    importlib.import_module(_COMMANDS[cmd]).main()
