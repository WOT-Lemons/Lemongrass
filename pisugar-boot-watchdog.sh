#/bin/bash
TMP=$(i2cget -y 1 0x57 0x06)
#echo $TMP
# Turn on the watchdog and feed the dog
RST=$((0x18 | TMP ))
#echo $RST
# Set the maximum number of restarts
i2cset -y 1 0x57 0x0a 10
# Write register
i2cset -y 1 0x57 0x06 $RST

# The script should be set to boot. You only need to run it once per boot
