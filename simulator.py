# subscriber.py
import time
import random
from datetime import datetime
import paho.mqtt.client as mqtt
import json
import threading
import datetime
 


def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    

client = mqtt.Client()
client.on_connect = on_connect
client.connect("broker.emqx.io", 1883, 60)


mqtt_topic = 'energy_meter/Raju/meter_id_01/readings'

v=random.randint(220,242)
i= random.randint(0,700)
c=0.8
p=v*i*c/1000
kwh= (p/1000)*3600
ade= (p/1000)*24*3600

def publish_it():
  #call this function every 5 seconds
  threading.Timer(60.0, publish_it).start()
  for j in range(1):
   for i in range(24):
      data =json.dumps({
          'voltage': v,
          'current': i,
          'Time': str(datetime.datetime.now() + datetime.timedelta(hours=i, days=j)),
          'Pf': c,
          'Power': p,
          'Current Hour Energy': kwh,
          'All Day Energy': ade
          });
      
      client.publish(mqtt_topic, data, qos=0, retain=False)
      
  #print the mqtt topic name and value on the command line console.
      print(f"send {data} to mqtt topic : {mqtt_topic}")
time.sleep(30)

#run the publish_it function {
client.loop_start()
publish_it()

client.loop_stop()
  

