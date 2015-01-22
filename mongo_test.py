from pymongo import Connection 
import psycopg2
import datetime
import glob
import os
import threading

conn = psycopg2.connect("dbname=test")
cur = conn.cursor()
cur.execute("DROP TABLE test")

# cur.execute("INSERT INTO test (username, t_date, t_time, lat,lng) VALUES (%s, %s, %s, %s, %s)",("Sean", "01/17","8.00",22.222,122.222))
start_time = datetime.datetime.now()
print "start_time is " + str(start_time)

def Postgresql():	
	cur.execute("CREATE TABLE test (id serial PRIMARY KEY, username varchar, t_date varchar, t_time varchar, lat float(24),lng float(24));")
	
	uid = {"000","001","002","003","004","005"}
	for id in uid:
		path = '/Geolife Trajectories 1.3/Data/'+id+'/Trajectory/'
		for filename in os.listdir(os.getcwd()+path):
			f = open(os.getcwd()+path+filename, 'r+')
			for line in f:
				d = line.split(",")
				if len(d) > 3:
					lat = d[0]
					lng = d[1]
					date = d[5]
					time = d[6]
					cur.execute("INSERT INTO test (username, t_date, t_time, lat,lng) VALUES (%s, %s, %s, %s, %s)",(id, date,time,lat,lng))
					
	end_time = datetime.datetime.now()
	print "Postgre total insert time : " + str(end_time - start_time)

	cur.execute("SELECT * From test WHERE username = '003' AND t_date = '2008-11-19' ORDER BY t_time;")
	end2_time = datetime.datetime.now()
	print "Postgre total query time : " + str(end2_time - end_time)
	
	cur.commit()
	cur.close()
	conn.close()
        

def Mongo():
	con = Connection()
	db = con.test
	posts = db.post
	uid = {"000","001","002","003","004","005"}
	for id in uid:
		path = '/Geolife Trajectories 1.3/Data/'+id+'/Trajectory/'
		for filename in os.listdir(os.getcwd()+path):
			f = open(os.getcwd()+path+filename, 'r+')
			for line in f:
				d = line.split(",")
				if len(d) > 3:
					lat = d[0]
					lng = d[1]
					date = d[5]
					time = d[6]
					post1 = {"user":id,"t_date": date,"t_time": time,"lat":lat,"lng":lng}
					posts.insert(post1)
					
	end_time = datetime.datetime.now()
	print "Mongo total insert time : " + str(end_time - start_time)

	post = posts.find({"user":"003","t_date":"2008-11-19"})
	post.sort([("t_time",1)])

	end2_time = datetime.datetime.now()
	print "Mongo total query time : " + str(end2_time - end_time)

thread1 = threading.Thread(target=Postgresql)
thread2 = threading.Thread(target=Mongo)
thread1.start()
thread2.start()
