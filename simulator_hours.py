import paho.mqtt.client as mqtt
import time
import json
import random
import datetime
import threading
import pandas as pd
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
    #print(result)

    energy={
            'current_hour_energy': message['Current Hour Energy'],
          'all_energy': message['All Day Energy'],
            'Time': message['Time']}
    result=firebase.post('/data/energy', energy)
    #print(result)

    

    '''kwh= {'Current Hour Energy': message['Current Hour Energy'],
        'Time': message['Time']
        }
    kwh= {k:[v] for k,v in kwh.items()}'''
    df1=df= pd.DataFrame(message, index=[0])
    df2=df.append(df1)
    data=df1.update(df2)
    data['Time'] = pd.to_datetime(data['Time'])
    #data.set_index(data['Time'], inplace= True)
    print(data)
    #df.groupby([pd.Grouper(freq='30min', key='Time'), 'Current Hour Energy']).sum()
    #print (df)
    #data=df.to_dict()
    #result=firebase.post('/data/hourly bins', data)
  
client=mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("broker.emqx.io", 1883, 60)
client.loop_forever()
