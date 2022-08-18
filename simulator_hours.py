import paho.mqtt.client as mqtt
import time
import json
import random
import datetime
import threading
from firebase import firebase  
firebase = firebase.FirebaseApplication('https://smart-29944-default-rtdb.firebaseio.com/', None)  

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("energy_meter/Raju/meter_id_01/#", qos=0)

def on_message(client, userdata, msg):
    
    data_in=str(msg.payload.decode("utf-8", "ignore"))
    
    message=json.loads(data_in)
    #print(message)

    data={
        'voltage': message['voltage'],
          'current': message['current'],
            'Pf': message['Pf'],
            'Time': message['Time'],
           'Power': message['Power']
            }
     
    result=firebase.post('/data/readings',data)
    print(result)

    energy={
            'Current Hour Energy': message['Current Hour Energy'],
          'All Day Energy': message['All Day Energy']}
    result=firebase.post('/data/energy', energy)
    print(result)
  
client=mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("broker.emqx.io", 1883, 60)
client.loop_forever()
