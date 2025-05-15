#%%
#!pip install paho-mqtt

# %%
import time
import paho.mqtt.client as paho
from paho import mqtt
import mqtt_credentials
import logging


logging.basicConfig(filename='/home/pi/infineon_co2_sensor/server/bt_and_mqtt_connection.log', filemode="w", format='%(asctime)s - %(message)s', level=logging.INFO)


class MqttClientHandler():
    def __init__(self,client_id,broker, port):
        print("Client_Name:" + client_id)

        self.client = paho.Client(client_id=client_id, protocol=paho.MQTTv5)
        
        # enable TLS for secure connection
        self.client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
        # set username and password
        self.client.username_pw_set(mqtt_credentials.MQTT_USER, mqtt_credentials.MQTT_USER)

        self.client.on_connect = self.on_connect
        #self.client.on_disconnect = self.on_disconnect
        #self.client.on_publish = self.on_publish

        # only connect after you get the data
        #self.client = self.connect_to_broker(broker, port)

        # TODO: Make the client reconnect
        # http://www.steves-internet-guide.com/loop-python-mqtt-client/
        #self.client.loop_forever()

        #clientloop_thread = threading.Thread(target=self.connect)
        #clientloop_thread.setDaemon(True)     
        #clientloop_thread.start()
        
    def connect_to_broker(self, broker, port):
        self.client.connect_async(host=broker, port=port, keepalive=120)
        #print("connect_to_broker triggered")
        # This connects, but does it reconnect?

        ## It should: http://www.steves-internet-guide.com/loop-python-mqtt-client/
        try:
            self.client.loop_start()
        except:
        #self.client.loop_forever()
            logging.warn("Inital MQTT Connection failed!") 
            print("Inital MQTT Connection failed!") 
        '''
        while not self.client.is_connected(): #TODO for ->fehlermeldung
            time.sleep(0.2)
            logging.warn("Inital MQTT Connection failed!") 
        '''

        return self.client   



    def on_disconnect(self, client, userdata, rc):

        client.connected_flag = False
        client.disconnect_flag = True

        # try to reconnect after disconnection
        self.client = self.connect_to_broker(self.broker, self.port)

        logging.warn("MQTT Disconnected") 

    '''
    #works
    def on_publish(self,client, userdata, msgID):
        print("published")
    '''

    def publish_payload(self, topic, payload):
        self.client.publish(topic, payload=payload, qos=1)

    # BUG: I dont know why I ahve to add those properties, did not find them in the docs
    def on_connect(self, client, userdata, flags, rc, properties):
        #print(type(client))
        if rc==0:
            print("connected OK Returned code=",rc)
            client.connected_flag = True  # set flag
            logging.info("MQTT Connected") 
        else:
            print("Bad connection Returned code=",rc)
            client.bad_connection_flag = True
            logging.warn("MQTT Connection failed!") 