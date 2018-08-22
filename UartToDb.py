import serial
from time import sleep
from influxdb import InfluxDBClient
import time

client = InfluxDBClient(host='localhost', port=8086)
client.create_database('ftest')
#client.get_list_database()
client.switch_database('ftest')
#client.create_retention_policy('oneday','24h',1, default=True)
#for 1st time only
client.query("CREATE CONTINUOUS QUERY maxtemp_cq ON ftest RESAMPLE EVERY 10s BEGIN SELECT max(temp) AS temp, x_axis, y_axis, z_axis, carId INTO ftest.onehr.newcar FROM ftest.oneday.car GROUP BY time(30s) END")
ser = serial.Serial ("/dev/ttyS0", 9600)    #Open port with baud rate


    

def sensor_val():
    while True:
        received_data = list(ser.read())              #read serial port
        sleep(0.03)
        data_left = ser.inWaiting()             #check for remaining byte
        received_data += ser.read(data_left)                   #print received data    ser.write(received_data)
        count=0
        for v in received_data:
            if count < 4 :
                if count == 0:
                    temp=v
                    #print("temp ",temp)
                if count == 1:
                    x=v
                    #print("x ",x)
                if count == 2:
                    y=v
                    #print("y ",y)
                if count == 3:
                    z=v
                    #print("z ",z)
            #print("\n---->")
                count=count+1
            else:
                return [{
			"measurement":"car", 
			"tags":{
				"carId":"chassiNO"
				},
			"fields":{
				"temp":float(temp),
				"x_axis":int(x),
				"y_axis":int(y),
				"z_axis":int(z)
			}
		}]

#list=sensor_val(ser)
#print(list[1])




def add_data_to_db(add_to_db):
    
    print("---------------------\nin add_to_db")
    print("\nData-\n",add_to_db)
    #client.switch_database('ftest')
    client.query("alter retention policy oneday on ftest duration 24h replication 1 default")
    #print("after read")
    #print(client.query("select * from car"))
    print("\nadded to influx:-\n",client.write_points(add_to_db))
    #add_to_db should be a list of dict

while True:
    data=sensor_val()
    print("from uart-", data)
    add_data_to_db(data)
    time.sleep(3)

