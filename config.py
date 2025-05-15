
# %% Setup Parameters You have to change for each room

# Room-Name
ROOM_NAME = "4C313"
# RASPI_MAC = "" 
# Sensor

SENSORS = [{"BT_TARGET_ADDRESSES" : "B8:27:EB:76:18:5E",
            "Room" : ROOM_NAME,
            "Sensor_Position" : "1.5m"}]



# %% Setup Parameters You will not have to change

# Target Connection Interval to Read Data from Sensor Nodes
MEASUREMENT_INTERVAL = 1000*60 # in ms