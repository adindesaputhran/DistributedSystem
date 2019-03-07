import socket, pickle
import multiprocessing as mp
import pandas as pd

def connect_node(ip,port,node,op):
	s = socket.socket()
	s.connect((ip, port))
	s.send(bytes(op,"utf-8"))
	# receive data from the server
	data = b""
	while True:
	    packet = s.recv(4096)
	    if not packet: break
	    data += packet

	df = pd.DataFrame(data=pickle.loads(data))
	print ("o/p from port "+str(node)+" is recieved")
	save = "output/data"+str(node)+".csv"
	df.to_csv(save)
	# close the connection
	s.close()

print("Number of processors: ", mp.cpu_count())

# Create a socket object
print("1)K-Means 2)DBSCAN 3)OPTICS\nEnter option:")
op = input();
# Define the port on which you want to connect
# port = 12345
procs = []
ip = [['127.0.0.1',65000],['127.0.0.1',1234]]
# connect to the server on local computer

# instantiating process with arguments
for i in ip:
# print(name)
	proc = mp.Process(target=connect_node, args=(i[0],i[1],i[1],op))
	procs.append(proc)
	proc.start()

# complete the processes
for proc in procs:
	proc.join()
