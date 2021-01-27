# camder
Camera Extraction Robot

## setup automount
```
git clone http://github.com/rbrito/usbmount.git
cd usbmount
sudo dpkg-buildpackage -us -uc -b
sudo dpkg -i ../usbmount_0.0.24_all.deb
```

## test the eInk display
```
sudo apt install python3-pip
sudo pip3 install adafruit-circuitpython-epd
```

Test code:
```python
#!/usr/bin/python3

import digitalio
import busio
import board

from adafruit_epd.epd import Adafruit_EPD
from adafruit_epd.ssd1675 import Adafruit_SSD1675

spi = busio.SPI(board.SCK, MOSI=board.MOSI, MISO=board.MISO)
ecs = digitalio.DigitalInOut(board.CE0)
dc = digitalio.DigitalInOut(board.D22)
rst = digitalio.DigitalInOut(board.D27)
busy = digitalio.DigitalInOut(board.D17)
srcs = None

display = Adafruit_SSD1675(122, 250, spi, cs_pin=ecs, dc_pin=dc, sramcs_pin=srcs,
                           rst_pin=rst, busy_pin=busy)
display.fill(Adafruit_EPD.WHITE)
    
display.fill_rect(0, 0, 50, 60, Adafruit_EPD.BLACK)
display.hline(80, 30, 60, Adafruit_EPD.BLACK)
display.vline(80, 30, 60, Adafruit_EPD.BLACK)
   
display.display()
```
