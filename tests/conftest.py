# obd and race_monitor are real, installed dependencies that import with no
# hardware or network side effects (only OBD/Async construction and
# RaceMonitorClient calls touch the outside world). Tests mock those I/O seams
# individually, so no suite-wide module mock is needed here.
