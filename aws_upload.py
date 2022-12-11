# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 1
device_end = 5

#Path to the dataset, modify this
data_path = "data2/vehicle{}.csv"

#Path to your certificates, modify this
certificate_formatter = "device_{}_certificate.pem.crt"
key_formatter = "device_{}_private.pem.key"

#certificate_formatter = "Green_Grass_Config/certificate.pem.crt"
#key_formatter = "Green_Grass_Config/private.pem.crt"


class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("aiz3ikhw59kmt-ats.iot.us-west-2.amazonaws.com", 8883)
        self.client.configureCredentials("AmazonRootCA1 (1).pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self,message):
        #TODO3: fill in the function to show your received message
        print("client {} received payload {} from topic {}".format(self.device_id, message.payload, message.topic))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self, Payload="payload"):
        #TODO4: fill in this function for your publish
        #self.client.subscribeAsync("TestTopic1", 0, ackCallback=self.customSubackCallback)
        
        #self.client.publishAsync("TestTopic2", Payload, 0, ackCallback=self.customPubackCallback)
        self.client.publishAsync("keyfinder", Payload, 0, ackCallback=self.customPubackCallback)



def upload_data (data_file, debug=False):
    print("Loading vehicle data...")
    data = pd.read_csv(data_file)

    device_id =0
    print("Initializing MQTTClients...")
    client = MQTTClient(0,certificate_formatter.format(device_id,device_id) ,key_formatter.format(device_id,device_id))
    #print(client)
    client.client.connect()
    #print(tst)
    #print(clients)

    max_row = data.shape[0]
    print("adding rows")
    for k in range(0,max_row):
            
        cd = data.iloc[k,].to_json()

        tst = client.publish(Payload=cd)


    print("all_done")
    client.client.disconnect()
    print("All devices disconnected")

