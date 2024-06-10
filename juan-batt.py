#!/usr/bin/env python
#todo: rewrite for telegraf >> influx
import time
import socket
from power_api import SixfabPower

api = SixfabPower()
now = int( time.time() )
pre = f"servers.{socket.gethostname()}."
CARBON_SERVER = '192.168.10.9'
CARBON_PORT = 2003


InputTemp = f"{pre}InputTemp {api.get_input_temp()} {now}\n"
InputVoltage = f"{pre}InputVoltage {api.get_input_voltage()} {now}\n"
InputCurrent = f"{pre}InputCurrent {api.get_input_current()} {now}\n"
InputPower = f"{pre}InputPower {api.get_input_power()} {now}\n"
SystemTemp = f"{pre}SystemTemp {api.get_system_temp()} {now}\n"
SystemVoltage = f"{pre}SystemVoltage {api.get_system_voltage()} {now}\n"
SystemCurrent = f"{pre}SystemCurrent {api.get_system_current()} {now}\n"
SystemPower = f"{pre}SystemPower {api.get_system_power()} {now}\n"
BatteryTemp = f"{pre}BatteryTemp {api.get_battery_temp()} {now}\n"
BatteryVoltage = f"{pre}BatteryVoltage {api.get_battery_voltage()} {now}\n"
BatteryCurrent = f"{pre}BatteryCurrent {api.get_battery_current()} {now}\n"
BatteryPower = f"{pre}BatteryPower {api.get_battery_power()} {now}\n"
BatteryLevel = f"{pre}BatteryLevel {api.get_battery_level()} {now}\n"
BatteryHealth = f"{pre}BatteryHealth {api.get_battery_health()} {now}\n"
FanHealth = f"{pre}FanHealth {api.get_fan_health()} {now}\n"
FanSpeed = f"{pre}FanSpeed {api.get_fan_speed()} {now}\n"

with socket.socket() as s:
    s.connect((CARBON_SERVER, CARBON_PORT))
    s.sendall(str.encode(InputTemp))
    s.sendall(str.encode(InputVoltage))
    s.sendall(str.encode(InputCurrent))
    s.sendall(str.encode(InputPower))
    s.sendall(str.encode(SystemTemp))
    s.sendall(str.encode(SystemVoltage))
    s.sendall(str.encode(SystemCurrent))
    s.sendall(str.encode(SystemPower))
    s.sendall(str.encode(BatteryTemp))
    s.sendall(str.encode(BatteryVoltage))
    s.sendall(str.encode(BatteryCurrent))
    s.sendall(str.encode(BatteryPower))
    s.sendall(str.encode(BatteryLevel))
    s.sendall(str.encode(BatteryHealth))
    s.sendall(str.encode(FanHealth))
    s.sendall(str.encode(FanSpeed))
