# %% packages to load

import logging

# Config file
import config

# Custom functions
from helpers import loop_through_sensors

import mqtt_credentials
from mqtt_handler import MqttClientHandler

# %% 

logging.basicConfig(filename='/home/pi/infineon_co2_sensor/bt_and_mqtt_connection.log', filemode="w", format='%(asctime)s - %(message)s', level=logging.INFO)


# Define MQTT Connection

mqtt_client_handler = MqttClientHandler("Test_Client",mqtt_credentials.MQTT_BROKER, mqtt_credentials.MQTT_PORT)
# Target Connection Interval to Read Data from Sensor Nodes
Measurement_Interval = config.MEASUREMENT_INTERVAL

# Read description of sensors in this room,
sensors = config.SENSORS



# %%
# Loop through all sensors in the config file
## see https://stackoverflow.com/questions/27033317/a-function-that-polls-on-intervals-and-yields-infinitely


import threading
import time
def poll(sensors, time_interval,termination_event):
    '''A function that polls data from all the sensors in a given time interval'''

    while True:
        loop_through_sensors(sensors, mqtt_client_handler)
        print(sensors)
        time.sleep(time_interval)
        
        '''
        if termination_event.is_set():
            break
        '''

poll(sensors, 30, None)

'''
event = threading.Event()
clientloop_thread = threading.Thread(target=poll,args=(sensors, 60000, event))
clientloop_thread.setDaemon(True)     
clientloop_thread.start()

logging.info("Started the script.") 


if input("Enter `q` to stop!") == "q":
    event.set()
'''
