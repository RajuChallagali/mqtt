# subscriber.py
import time
import pandas as pd
from datetime import datetime
import paho.mqtt.client as mqtt
import json
from firebase import firebase  
firebase = firebase.FirebaseApplication('https://smart-29944-default-rtdb.firebaseio.com/', None)  



def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    # subscribe, which need to put into on_connect
    # if reconnect after losing the connection with the broker, it will continue to subscribe to the raspberry/topic topic
    # client.subscribe("IoTSolutions/#")
    #client.subscribe("MK117-16d0/#")
    client.subscribe("MK117-3e04/04/device_to_app/#")
# the callback function, it will be triggered when receiving messages 
def on_message(client, userdata, msg):
    json_object= str(msg.payload.decode("utf-8"))
    #recieved message is converted to as json object
    message = json.loads(json_object)
    print(message)
    #json object is loaded to variable called message
#We have three message IDs, in order to get required data, we use if else statement here
    if message['msg_id'] ==1006:
        
        data ={ 
            'voltage': message['data']['voltage'],
          'current': message['data']['current'],
          'power': message['data']['power'],
            'Pf': message['data']['power_factor'],
            'Time': datetime.now(),
            'time': message['data']['timestamp']
          }
        time.sleep(1)  


        result = firebase.post('/data/readings',data)
        #for message id 1006, we have created a folder in readings/1006
        print(result)
        
    elif message['msg_id'] ==1001:  
        
        data ={ 
            'switch_state': message['data']['switch_state'],  
          'overload_state': message['data']['overload_state'],  
          'overcurrent_state': message['data']['overcurrent_state'],
        'overvoltage_state': message['data']['overvoltage_state'],
           'Time': datetime.now(),
            'time': message['data']['timestamp']
          }  
        time.sleep(10)  
        now = datetime.now()

        result = firebase.post('/data/switch_state',data)
        #for message id 1001, we have created a folder in readings/1001 
        print(result) 
        
    else:
        data ={ 
            'EC': message['data']['EC'],  
          'all_energy': message['data']['all_energy'],  
          'thirty_day_energy': message['data']['thirty_day_energy'],
        'current_hour_energy': message['data']['current_hour_energy'],
           'Time': datetime.now(),
            'time': message['data']['timestamp']
          }  
        time.sleep(10)  
        now = datetime.now()

        result = firebase.post('/data/energy',data)
        #it is default message id 1018,for message id 1018, we have created a folder in readings/1018 
        print(result) 

    #msg=message['data']
    """df1=df= pd.DataFrame(msg, index= [0])
    df2=df.append(df1)
    df2['timestamp']=s=message['data']['timestamp']
    dt=datetime.fromisoformat(s)
    dt1 = pd.to_datetime(dt, errors='coerce')
    df2.set_index(dt1)
    print(df2)"""

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# set the will message, when the Raspberry Pi is powered off, or the network is interrupted abnormally, it will send the will message to other clients
# client.will_set('raspberry/status', b'{"status": "Off"}')

# create connection, the three parameters are broker address, broker port number, and keep-alive time respectively
client.connect("broker.emqx.io", 1883, 60)

# set the network loop blocking, it will not actively end the program before calling disconnect() or the program crash
client.loop_forever()
