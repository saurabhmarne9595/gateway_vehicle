
# import\ required file ------------------- 
from influxdb import InfluxDBClient
import pika
import sys
import json
import time


# declarations--------------------
client = InfluxDBClient(host='localhost', port=8086)
#def send_data_to_server(data):
cred=pika.PlainCredentials('admin', 'password')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='34.224.98.65',credentials=cred))
print("connection-",connection)
channel = connection.channel()
channel.queue_declare(queue='local_queue', durable=True)


# code----------------------
while True:
    time.sleep(10)
    data = read_from_db()
    send_data_to_server(data)



def read_from_db():
    print("----------------------\nin read from db")
    #client.switch_database('ftest')
    client.query("alter retention policy onehr on ftest duration 1h replication 1 default")
    rs = client.query("SELECT * from newcar")
    #print(rs)
    points =list(rs.get_points(measurement='newcar', tags={'carId': 'chassiNO'}))
    top_item=(len(points)-1)
    #print(points)
    print("type",type(points[0])) #should be dict
    carId=points[top_item]['carId']
    temp=points[top_item]['max']
    Xaxis=points[top_item]['X_axis']
    Yaxis=points[top_item]['Y_axis']
    Zaxis=points[top_item]['Z_axis']
    json_send={
        "measurement":"car",
        "tags":{
            "carId":carId
            },
        "fields":{
            "temp":float(temp),
            "Xaxis":int(Xaxis),
            "Yaxis":int(Yaxis),
            "Zaxis":int(Zaxis)
            }
        }
    print("return from influx-",json_send)
    return(json_send)


def send_data_to_server(data):
    print("\nsend_data_to_server",data)
    res=channel.basic_publish(exchange='',
                      routing_key='local_queue',
                      #data should be of dict type
                      body=json.dumps(data),
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                                ))
    print(res)

#connection.close()
#send_data_to_server("hey")


