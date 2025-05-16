import logging
import time
import csv
import os
import paho.mqtt.client as paho
from paho import mqtt # Obwohl mqtt.client.ssl.PROTOCOL_TLS nicht direkt verwendet wird, behalte ich den Import für den Fall, dass TLS später benötigt wird.
from bluepy import btle
import traceback # Importiert, aber implizit durch exc_info=True in logging verwendet

# Configure logging
# Stelle sicher, dass der Benutzer, der das Skript ausführt, Schreibrechte für diese Datei hat.
# Für einen Cronjob ist es oft besser, absolute Pfade zu verwenden.
LOG_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sensor_mqtt.log')
logging.basicConfig(
    filename=LOG_FILE_PATH,
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO # Ändere dies zu logging.DEBUG, um detailliertere BTLE-Ausgaben zu sehen
)
logger = logging.getLogger(__name__)

# Configuration
ROOM_NAME = "4C313"
MEASUREMENT_INTERVAL = 30  # seconds
SENSORS = [
    {
        "BT_TARGET_ADDRESSES": "B8:27:EB:76:18:5E", # Beispiel MAC-Adresse, ersetzen durch echte Sensor-MAC
        "Room": ROOM_NAME,
        "Sensor_Position": "1.5m"
    }
    # Hier können weitere Sensoren hinzugefügt werden
]
MQTT_BROKER = '158.180.44.'
MQTT_PORT = 1883 # Standard MQTT Port (unverschlüsselt)
# MQTT_PORT = 8883 # Standard MQTT Port (verschlüsselt)
MQTT_USER = "bobm"
MQTT_PASSWORD = ""letmein
MQTT_CLIENT_ID = f"Sensor_Client_{ROOM_NAME}" # Eindeutigerer Client-ID

# MQTT Client Handler
class MqttClientHandler:
    def __init__(self, client_id, broker, port, username, password):
        self.broker = broker
        self.port = port
        self.username = username
        self.password = password
        self.client = paho.Client(client_id=client_id, protocol=paho.MQTTv5)
        
        # TLS-Konfiguration:
        # Wenn dein Broker TLS erfordert (z.B. auf Port 8883), entkommentiere und konfiguriere dies.
        # Für Port 1883 ist TLS unüblich.
        # if self.port == 8883: # Beispielbedingung für TLS
        # try:
        # self.client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
        # # Optional: Pfade zu Zertifikaten, wenn benötigt
        # # self.client.tls_set(ca_certs="path/to/ca.crt",
        # # certfile="path/to/client.crt",
        # # keyfile="path/to/client.key",
        # # tls_version=mqtt.client.ssl.PROTOCOL_TLS)
        # logger.info("TLS is configured for MQTT connection.")
        # except ImportError:
        # logger.error("ssl module not found, TLS cannot be configured. Install paho-mqtt[ssl].")
        # except Exception as e:
        # logger.error(f"Error setting TLS: {e}")


        self.client.username_pw_set(username, password)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self._is_connected_flag = False

    def on_connect(self, client, userdata, flags, reasonCode, properties=None):
        # Beachte: Paho MQTT v1.x verwendet 'reasonCode', v2.x verwendet 'reason_code'.
        # Passe dies ggf. an deine Paho-Version an.
        if reasonCode == 0:
            logger.info(f"Successfully connected to MQTT Broker {self.broker}:{self.port}")
            self._is_connected_flag = True
        else:
            logger.error(f"Failed to connect to MQTT Broker. Reason code: {reasonCode}")
            self._is_connected_flag = False

    def on_disconnect(self, client, userdata, reasonCode, properties=None):
        logger.warning(f"Disconnected from MQTT Broker. Reason code: {reasonCode}")
        self._is_connected_flag = False
        # Hier könnte eine Logik für automatische Wiederverbindungsversuche implementiert werden,
        # obwohl die Hauptschleife bereits Wiederverbindungsversuche unternimmt.

    def connect(self):
        if self._is_connected_flag:
            logger.info("Already connected to MQTT Broker.")
            return True
        try:
            logger.info(f"Attempting to connect to MQTT Broker: {self.broker}:{self.port}")
            self.client.connect_async(self.broker, self.port, keepalive=120)
            self.client.loop_start() # Startet einen Thread für Netzwerk-Traffic und Callbacks
            
            timeout = time.time() + 15  # 15 Sekunden Timeout für die Verbindung
            while not self._is_connected_flag and time.time() < timeout:
                time.sleep(0.2)
            
            if not self._is_connected_flag:
                logger.error("Failed to connect to MQTT Broker within timeout period.")
                self.client.loop_stop() # Stoppe den Loop, wenn die Verbindung fehlschlägt
                return False
            
            logger.info("MQTT connection established and loop started.")
            return True
        except Exception as e:
            logger.error(f"MQTT connection error: {str(e)}", exc_info=True)
            if self.client.is_connected(): # Sicherstellen, dass der Loop gestoppt wird, falls connect_async teilweise erfolgreich war
                 self.client.loop_stop(force=True)
            return False

    def publish(self, topic, payload, retain=False):
        if self._is_connected_flag:
            try:
                result = self.client.publish(topic, payload, qos=1, retain=retain) # QoS 1 für "mindestens einmal"
                result.wait_for_publish(timeout=5) # Warte auf Bestätigung (für QoS > 0)
                if result.rc == paho.MQTT_ERR_SUCCESS:
                    logger.info(f"Successfully published to {topic}: {payload}")
                else:
                    logger.error(f"Failed to publish to {topic}. MQTT Error Code: {result.rc}")
            except Exception as e:
                logger.error(f"Exception during publish to {topic}: {e}", exc_info=True)
        else:
            logger.warning(f"Cannot publish to {topic}: MQTT not connected.")

    def disconnect(self):
        if self._is_connected_flag or self.client.is_connected():
             logger.info("Disconnecting from MQTT Broker.")
             self.client.loop_stop() # Stoppt den Netzwerk-Thread sauber
             self.client.disconnect()
             logger.info("MQTT client disconnected.")
        self._is_connected_flag = False


# Sensor Data Collection
def get_sensor_data(sensor_mac_address):
    peripheral = None
    retries = 3
    retry_delay = 5 # Sekunden

    for attempt in range(retries):
        try:
            logger.info(f"Attempting to connect to BTLE sensor {sensor_mac_address} (Attempt {attempt + 1}/{retries})")
            # Hier könnte eine spezifischere Interface-Auswahl nötig sein, z.B. peripheral = btle.Peripheral(sensor_mac_address, "hci0")
            peripheral = btle.Peripheral(sensor_mac_address)
            logger.info(f"Connected to BTLE sensor {sensor_mac_address}")

            # BLE UUIDs (diese müssen exakt zu deinem Sensor passen)
            MEASUREMENTS_SERVICE_UUID = btle.UUID("2a13dada-295d-f7af-064f-28eac027639f")
            CO2_UUID = btle.UUID("4ef31e63-93b4-eca8-3846-84684719c484")
            PRESS_UUID = btle.UUID("0b4f4b0c-0795-1fab-a44d-ab5297a9d33b")
            TEMP_UUID = btle.UUID("7eb330af-8c43-f0ab-8e41-dc2adb4a3ce4")
            HUM_UUID = btle.UUID("421da449-112f-44b6-4743-5c5a7e9c9a1f")

            service = peripheral.getServiceByUUID(MEASUREMENTS_SERVICE_UUID)

            # Read characteristics
            co2_char = service.getCharacteristics(CO2_UUID)[0]
            press_char = service.getCharacteristics(PRESS_UUID)[0]
            temp_char = service.getCharacteristics(TEMP_UUID)[0]
            hum_char = service.getCharacteristics(HUM_UUID)[0]

            # CO2
            co2_reading = co2_char.read()
            co2_raw = int.from_bytes(co2_reading, byteorder='little', signed=False) # Sicherere Konvertierung
            logger.debug(f"CO2 raw bytes: {co2_reading.hex()}, Value: {co2_raw} ppm")
            co2 = {"name": "co2_ppm", "unit": "ppm", "value": co2_raw}

            # Pressure
            press_reading = press_char.read()
            press_raw = int.from_bytes(press_reading, byteorder='little', signed=False) # Sicherere Konvertierung
            logger.debug(f"Pressure raw bytes: {press_reading.hex()}, Value: {press_raw} Pa")
            # Einheit korrigiert und Name angepasst
            pressure = {"name": "pressure_Pa", "unit": "Pa", "value": press_raw} 

            # Temperature
            temp_reading = temp_char.read()
            # Annahme: Wert ist signed int16, geteilt durch 100 für °C
            # Beispiel: temp_value_raw = int.from_bytes(temp_reading, byteorder='little', signed=True)
            # temp_celsius = temp_value_raw / 100.0
            # Die ursprüngliche Logik für Temperatur und Feuchtigkeit war komplex.
            # Überprüfe die Datenblatt-Spezifikation deines Sensors genau.
            # Hier ist die ursprüngliche Logik, ggf. anpassen:
            temp_raw_high = (temp_reading[1] << 8) + temp_reading[0]
            temp_raw_low = (temp_reading[3] << 8) + temp_reading[2] # Dies ist unüblich für Temp/Hum
            temp_raw = temp_raw_high + (temp_raw_low / 1000.0) # Division durch float sicherstellen
            logger.debug(f"Temperature raw bytes: {temp_reading.hex()}, Value: {temp_raw} °C")
            temp = {"name": "temperature_celsius", "unit": "°C", "value": round(temp_raw, 2)}

            # Humidity
            hum_reading = hum_char.read()
            # Ähnlich wie Temperatur, genaue Byte-Interpretation prüfen.
            # Hier ist die ursprüngliche Logik, ggf. anpassen:
            hum_raw_high = (hum_reading[1] << 8) + hum_reading[0]
            hum_raw_low = (hum_reading[3] << 8) + hum_reading[2] # Dies ist unüblich für Temp/Hum
            hum_raw = hum_raw_high + (hum_raw_low / 1000.0) # Division durch float sicherstellen
            logger.debug(f"Humidity raw bytes: {hum_reading.hex()}, Value: {hum_raw} %rH")
            hum = {"name": "humidity_percent", "unit": "%rH", "value": round(hum_raw, 2)}
            
            data_list = [co2, pressure, temp, hum]
            measurement = {
                "CO2_ppm": co2_raw,
                "Pressure_Pa": press_raw,
                "Temperature_Celsius": round(temp_raw, 2),
                "Humidity_Percent": round(hum_raw, 2)
            }
            return data_list, measurement

        except btle.BTLEDisconnectError as e:
            logger.error(f"BTLEDisconnectError for sensor {sensor_mac_address} (Attempt {attempt + 1}): {str(e)}", exc_info=False) # exc_info=False, da es erwartet werden kann
            if attempt < retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to sensor {sensor_mac_address} after {retries} attempts.")
                return [], {"CO2_ppm": None, "Pressure_Pa": None, "Temperature_Celsius": None, "Humidity_Percent": None}
        except Exception as e:
            logger.error(f"Failed to read sensor {sensor_mac_address}: {str(e)}", exc_info=True)
            return [], {"CO2_ppm": None, "Pressure_Pa": None, "Temperature_Celsius": None, "Humidity_Percent": None}
        finally:
            if peripheral:
                try:
                    peripheral.disconnect()
                    logger.info(f"Disconnected from BTLE sensor {sensor_mac_address}")
                except Exception as e_disconnect:
                    logger.error(f"Error disconnecting from sensor {sensor_mac_address}: {e_disconnect}", exc_info=True)

# Data Transformation
def add_meta_information(sensor_config, measurement_data):
    # Stellt sicher, dass measurement_data nicht None ist und ein Wörterbuch ist
    if not isinstance(measurement_data, dict):
        logger.error("Invalid measurement_data for meta information: not a dictionary.")
        return {} # Gibt ein leeres Wörterbuch zurück oder behandelt den Fehler entsprechend

    # Kopiere das Wörterbuch, um das Original nicht zu verändern, falls es woanders verwendet wird
    transformed_measurement = measurement_data.copy()
    
    transformed_measurement["Room"] = sensor_config.get("Room", "UnknownRoom")
    transformed_measurement["Position"] = sensor_config.get("Sensor_Position", "UnknownPosition")
    transformed_measurement["Sensor_ID_MAC"] = sensor_config.get("BT_TARGET_ADDRESSES", "UnknownMAC")
    transformed_measurement["timestamp_utc"] = int(time.time()) # Unix-Timestamp (Sekunden seit Epoche)
    transformed_measurement["datetime_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) # ISO 8601 Format

    return transformed_measurement

# Write to CSV
def write_to_csv(measurement_data, base_path="/home/pi/infineon_co2_sensor/server/"):
    if not measurement_data or not measurement_data.get("Room"):
        logger.error("Cannot write to CSV: measurement data is invalid or Room is missing.")
        return

    # Erstelle den Zielordner, falls er nicht existiert
    try:
        # Stelle sicher, dass der Basispfad existiert
        os.makedirs(base_path, exist_ok=True)
    except OSError as e:
        logger.error(f"Error creating directory {base_path}: {e}")
        return # Beende, wenn das Verzeichnis nicht erstellt werden kann

    csv_file_path = os.path.join(base_path, f"{measurement_data['Room']}.csv")
    
    # Überprüfe, ob alle Werte None sind (passiert, wenn Sensor nicht gelesen werden konnte)
    # In diesem Fall wollen wir vielleicht keine Zeile schreiben oder eine Zeile mit leeren Werten.
    # Hier entscheiden wir uns, keine Zeile zu schreiben, wenn alle Kernmesswerte None sind.
    core_values = [
        measurement_data.get("CO2_ppm"),
        measurement_data.get("Pressure_Pa"),
        measurement_data.get("Temperature_Celsius"),
        measurement_data.get("Humidity_Percent")
    ]
    if all(v is None for v in core_values):
        logger.info(f"Skipping CSV write for {measurement_data.get('Sensor_ID_MAC')} as all sensor values are None.")
        return

    file_exists = os.path.exists(csv_file_path)
    
    try:
        with open(csv_file_path, mode='a', newline='', encoding='utf-8') as csvfile:
            # Definiere die Feldnamen basierend auf den Schlüsseln in measurement_data
            # Es ist gut, eine konsistente Reihenfolge zu haben.
            fieldnames = [
                "datetime_utc", "timestamp_utc", "Room", "Position", "Sensor_ID_MAC",
                "CO2_ppm", "Temperature_Celsius", "Humidity_Percent", "Pressure_Pa"
            ]
            # Stelle sicher, dass alle Schlüssel aus measurement_data in fieldnames sind,
            # oder füge sie dynamisch hinzu (kann zu inkonsistenten Spalten führen, wenn sich Daten ändern)
            # Für Konsistenz ist es besser, fieldnames explizit zu definieren.
            
            # Filtere measurement_data, um nur die in fieldnames definierten Spalten zu schreiben
            filtered_measurement_data = {key: measurement_data.get(key) for key in fieldnames}

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(filtered_measurement_data)
        logger.info(f"Successfully wrote data to {csv_file_path}")
    except IOError as e:
        logger.error(f"CSV write error to {csv_file_path}: {str(e)}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred during CSV writing to {csv_file_path}: {str(e)}", exc_info=True)


# Main Loop
def main():
    logger.info("Starting sensor data collection script.")
    mqtt_handler = MqttClientHandler(MQTT_CLIENT_ID, MQTT_BROKER, MQTT_PORT, MQTT_USER, MQTT_PASSWORD)
    
    # Erster Verbindungsversuch beim Start
    if not mqtt_handler.connect():
        logger.warning("Initial MQTT connection failed. Will retry in the loop.")

    try:
        while True:
            for sensor_config in SENSORS:
                sensor_mac = sensor_config["BT_TARGET_ADDRESSES"]
                logger.info(f"Processing sensor: {sensor_mac} in Room: {sensor_config['Room']}")
                
                # Daten vom Sensor abrufen
                # data_list enthält einzelne Metriken für MQTT, base_measurement enthält Rohdaten für CSV
                data_list_for_mqtt, base_measurement_for_csv = get_sensor_data(sensor_mac)

                if not base_measurement_for_csv or all(v is None for v in base_measurement_for_csv.values()):
                    logger.warning(f"No valid data received from sensor {sensor_mac}. Skipping MQTT and CSV for this cycle.")
                    # Optional: Wartezeit erhöhen oder spezifische Fehlerbehandlung
                    time.sleep(5) # Kurze Pause, bevor der nächste Sensor oder Zyklus beginnt
                    continue

                # Metadaten hinzufügen (für CSV und ggf. für einen aggregierten MQTT-Payload)
                full_measurement_data = add_meta_information(sensor_config, base_measurement_for_csv)

                # Daten in CSV schreiben
                # Stelle sicher, dass der Pfad für den Cronjob korrekt ist (z.B. /home/pi/...)
                # Der Pfad wird jetzt in write_to_csv selbst gehandhabt
                csv_base_path = os.path.join(os.path.expanduser("~"), "infineon_co2_sensor", "server") # z.B. /home/pi/infineon_co2_sensor/server/
                write_to_csv(full_measurement_data, base_path=csv_base_path)

                # Überprüfe MQTT-Verbindung und verbinde ggf. neu
                if not mqtt_handler._is_connected_flag: # Zugriff auf internes Flag für schnelle Prüfung
                    logger.info("MQTT disconnected. Attempting to reconnect...")
                    mqtt_handler.connect() # connect() hat bereits Logik für "already connected"

                # Daten an MQTT senden, wenn verbunden und Daten vorhanden sind
                if mqtt_handler._is_connected_flag and data_list_for_mqtt:
                    for data_item in data_list_for_mqtt:
                        if data_item["value"] is not None: # Sende nur, wenn ein Wert vorhanden ist
                            # Topic-Struktur: bus/ROOM_NAME/SENSOR_MAC/METRIC_NAME
                            topic = f"bus/{full_measurement_data['Room']}/{sensor_mac.replace(':', '')}/{data_item['name']}"
                            payload = str(data_item["value"])
                            mqtt_handler.publish(topic, payload)
                        else:
                            logger.debug(f"Skipping MQTT publish for {data_item['name']} from sensor {sensor_mac} due to None value.")
                elif not data_list_for_mqtt:
                     logger.warning(f"No data in data_list_for_mqtt for sensor {sensor_mac} to publish via MQTT.")
                else: # Nicht verbunden
                    logger.warning(f"Cannot send data for sensor {sensor_mac} via MQTT: Not connected.")
            
            logger.info(f"All sensors processed. Waiting for {MEASUREMENT_INTERVAL} seconds before next cycle.")
            time.sleep(MEASUREMENT_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Script terminated by user (KeyboardInterrupt).")
    except Exception as e:
        logger.error(f"Unhandled script error in main loop: {str(e)}", exc_info=True)
    finally:
        logger.info("Shutting down script.")
        if 'mqtt_handler' in locals() and mqtt_handler:
            mqtt_handler.disconnect()
        logger.info("Script shutdown complete.")

if __name__ == "__main__":
    # Wichtig für Cronjobs: Stelle sicher, dass das Skript mit dem richtigen Python-Interpreter
    # und im richtigen Arbeitsverzeichnis ausgeführt wird, oder verwende absolute Pfade für alles.
    # Beispiel für Cronjob-Eintrag:
    # */5 * * * * /usr/bin/python3 /pfad/zum/skript/dein_skript.py >> /pfad/zum/skript/cron.log 2>&1
    #
    # Für bluepy unter Linux ohne root:
    # 1. sudo apt-get install libglib2.0-dev
    # 2. sudo setcap 'cap_net_raw,cap_net_admin+eip' $(readlink -f $(which python3))
    #    (oder spezifischer für den Python-Interpreter, den du verwendest)
    # Dies muss nach jedem Python-Update wiederholt werden.
    # Alternativ: Udev-Regeln für den BT-Adapter.
    main()
