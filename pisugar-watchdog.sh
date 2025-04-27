#/bin/bash
TMP=$(i2cget -y 1 0x57 0x06)
# echo $TMP
# WatchdogOn
RST=$((0x80 | TMP ))
# echo $RST
# 10*2s Set timeout duration 10 * 2S
i2cset -y 1 0x57 0x07 10
# Write register
i2cset -y 1 0x57 0x06 $RST
# i2cdump -y 1 0x57


# You need to feed the dog regularly, otherwise the system will restart
while [ 1 ]; do
    TMP=$(i2cget -y 1 0x57 0x06)
    #echo $TMP  >> /home/pi/wdlog.txt
    # Make sure the watchdog is on
    RST=$((0x80 | TMP ))
    # feed watchdog
    RST=$((0x20 | TMP ))
    # echo $RST  >> /home/pi/wdlog.txt
    i2cset -y 1 0x57 0x06 $RST
    # i2cget -y 1 0x57 0x06  >> /home/pi/wdlog.txt
    # i2cdump -y 1 0x57 >> /home/pi/wdlog.txt
    sleep 1
    # Once a second
done
