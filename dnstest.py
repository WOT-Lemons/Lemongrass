#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import re
import time

output = subprocess.check_output(['dig', '@8.8.8.8', '+short', 'TXT', 'driver.wotlemons.com'])
print(output)
print(str(output))
dirty_driver = output.decode("utf-8")
print(dirty_driver)
current_driver = dirty_driver.replace('"', '')
print(current_driver)

