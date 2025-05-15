
from mqtt_handler import MqttClientHandler

import mqtt_credentials

def loop_through_sensors(sensors, mqtt_client_handler):

    ''' A function that loop through a dictionary of sensors defined in the config file'''

    for sensor in sensors:
        ID = sensor["BT_TARGET_ADDRESSES"]
        print("Trying to get data from {}".format(ID))

        try:
            datalist, measurement = get_data_from_sensor(ID)
        except:
            print("Could not reach {}".format(ID))
            measurement = {"CO2" : None, "Pressure" : None, "Temperature" : None}
            datalist = []
            # TODO: Add logging

        measurement = add_meta_information(sensor, measurement)
        print(measurement)
        # Write data locally
        write_to_file(measurement)

        if not mqtt_client_handler.client.is_connected():
            try:
                mqtt_client_handler.client = mqtt_client_handler.connect_to_broker(mqtt_credentials.MQTT_BROKER, mqtt_credentials.MQTT_PORT)
            except:
                 print("MQTT failed at reconnect!")
        #TODO: Write data externally
        
        if mqtt_client_handler.client.is_connected():
            if datalist:
                print("MQTT connected. Sending the data for {}".format(ID))
                for i in datalist:
                    #topic = prefix + "/" + device + "/" + mac + "/" + i["name"]
                    topic = "cypress_hill" + "/" + measurement["Room"] + "/" + ID + "/" + i["name"]
                    #print(i)
                    mqtt_client_handler.publish_payload(topic, str(i["value"]))

# %%


import csv
import os

def write_to_file(measurement):

    '''A function that takes the measurement dict an writes it to a CSV-file named like the room'''
    csv_file =  "/home/pi/infineon_co2_sensor/server/{}.csv".format(measurement["Room"])

    if os.path.exists(csv_file):
        append_write = 'a' # append if already exists
    else:
        append_write = 'w' # make a new file if not
        
    try:
        with open(csv_file, append_write, newline='') as csvfile:
            w = csv.DictWriter(csvfile, measurement.keys())

            # only create header is file was just created
            # Caution: This is hacky, I do not really know why 26 works (might be shorter the length of the header)
            if append_write == 'w':
                w.writeheader()
            w.writerow(measurement)
    except IOError:
        print("I/O error")

#measurement = {"Room":"Room1", "CO2" : 111, "Pressure" : 11, "Temperature" : 11.22}
#write_to_file(measurement)
#measurement = {"Room":"Room1", "CO2" : None, "Pressure" : None, "Temperature" : None}
#write_to_file(measurement)

##
# 

# %%
import time
def add_meta_information(sensor, measurement):
    ''' A function that takes measurement dict and adds key value pairs from the config file to describe the data point.
    Returns a longer dict.'''
    measurement["Room"] = sensor["Room"]
    measurement["Position"] = sensor["Sensor_Position"] 
    measurement["Sensor ID"] = sensor["BT_TARGET_ADDRESSES"] 
    measurement["time"] = int( time.time() )

    return measurement



from bluepy import btle
import logging
import traceback
logging.basicConfig(filename="/home/pi/infineon_co2_sensor/server/bt_and_mqtt_connection.log", filemode="w", format='%(asctime)s - %(message)s', level=logging.INFO)

def get_data_from_sensor(ID):

    ''' A function that take the Bluetooth-ID of a device, connects an returns a list with a dictionary with the measurement '''
    try:
        # BLE Service & Characteristic UUIDs - Do NOT Change Anything
        MEASUREMENTS_SERVICE_UUID = btle.UUID("2a13dada-295d-f7af-064f-28eac027639f")
        CO2_DATA_CHARACTERISTIC_UUID = btle.UUID("4ef31e63-93b4-eca8-3846-84684719c484")
        PRESS_DATA_CHARACTERISTIC_UUID = btle.UUID("0b4f4b0c-0795-1fab-a44d-ab5297a9d33b")
        TEMP_DATA_CHARACTERISTIC_UUID = btle.UUID("7eb330af-8c43-f0ab-8e41-dc2adb4a3ce4")
        HUM_DATA_CHARACTERISTIC_UUID = btle.UUID("421da449-112f-44b6-4743-5c5a7e9c9a1f")

        # SETTINGS_SERVICE_UUID = btle.UUID("2119458a-f72c-269b-4d4d-2df0319121dd")
        # SAMPLE_RATE_CHARACTERISTIC_UUID = btle.UUID("8420e6c6-49ba-7c8d-104f-10fe496d061f")
        # ALARM_THRESHOLD_CHARACTERISTIC_UUID = btle.UUID("4ffb7e99-85ba-de86-4242-004f76f23409")python
        # SOLDERING_COMPENSATION_CHARACTERISTIC_UUID = btle.UUID("6f8afe94-a93d-cfb2-1b47-da0f98d9bfa1")

        # Connect sensorboard by ID
        XENSIV_BLE_Adapter = btle.Peripheral(ID)
        
        # Get services from this board
        Measurements_Service = XENSIV_BLE_Adapter.getServiceByUUID(MEASUREMENTS_SERVICE_UUID)

        # Get characteristics from service
        CO2_Data_Characteristic = Measurements_Service.getCharacteristics(CO2_DATA_CHARACTERISTIC_UUID)[0]
        Press_Data_Characteristic = Measurements_Service.getCharacteristics(PRESS_DATA_CHARACTERISTIC_UUID)[0]
        Temp_Data_Characteristic = Measurements_Service.getCharacteristics(TEMP_DATA_CHARACTERISTIC_UUID)[0]
        Hum_Data_Characteristic = Measurements_Service.getCharacteristics(HUM_DATA_CHARACTERISTIC_UUID)[0]


        # Apply bit-shifting, to get concentration in ppm
        CO2_Reading = CO2_Data_Characteristic.read()
        CO2_Raw = (CO2_Reading[1] << 8) + CO2_Reading[0]
        # print("CO2 Concentration: %s ppm" % CO2_Raw)
        co2 = {"name": "co2_ppm", "unit": "integer", "value": CO2_Raw}
        logging.debug("CO2 Concentration: %s ppm" % CO2_Raw)


        # Apply bit-shifting, to get pressure in ppm
        Press_Reading = Press_Data_Characteristic.read()
        Press_Raw = (Press_Reading[3] << 24) + (Press_Reading[2] << 16) + (Press_Reading[1] << 8) + Press_Reading[0]
        # print("Pressure: %s Pa" % Press_Raw)
        logging.debug("Pressure: %s Pa" % Press_Raw)
        pressure = {"name": "pressure", "unit": "percent", "value": Press_Raw}

        # Apply bit-shifting, to get temperature in °C
        # Temp_Raw_High is what comes before the decimal point
        # Temp_Raw_High is what comes after the decimal point
        Temp_Reading = Temp_Data_Characteristic.read()
        Temp_Raw_High = (Temp_Reading[1] << 8) + Temp_Reading[0]
        Temp_Raw_Low = (Temp_Reading[3] << 8) + Temp_Reading[2]
        Temp_Raw = Temp_Raw_High + (Temp_Raw_Low / 1000)
        # print("Temperature: %s °C" % Temp_Raw)
        logging.debug("Temperature: %s °C" % Temp_Raw)
        temp = {"name": "temperature", "unit": "float", "value": Temp_Raw}


        Hum_Reading = Hum_Data_Characteristic.read()
        Hum_Raw_High = (Hum_Reading[1] << 8) + Hum_Reading[0]
        Hum_Raw_Low = (Hum_Reading[3] << 8) + Hum_Reading[2]
        Hum_Raw = Hum_Raw_High + (Hum_Raw_Low / 1000)
        #print(Hum_Raw)
        logging.debug("Humidity: %s rF " % Hum_Raw)
        hum = {"name": "humidity", "unit": "percent", "value": Hum_Raw}

   
        data_list = []
        data_list.append(co2)
        data_list.append(pressure)
        data_list.append(temp)
        data_list.append(hum)              

        XENSIV_BLE_Adapter.disconnect()

        #return data_list
        return data_list, {"CO2" : int(CO2_Raw), "Pressure" : int(Press_Raw), "Temperature" : float(Temp_Raw), "Humidity" : float(Hum_Raw)}

    except:
        print(traceback.format_exc())
        logging.error("Exception captured ", exc_info=True)



