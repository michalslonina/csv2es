#!/usr/bin/python3
import requests
import sys
import csv
import json
import time
import multiprocessing
import queue
import threading

def prepare_data(index_name, index_schema, lines, headers, offset):
	docid=offset
	out=""
	for line in lines:
		vals = line.split(",")
		obj = dict(zip(headers,vals))
		out+='{"index":{"_index":"'+index_name+'","_type":"'+index_schema+'","_id":"'+str(docid)+'"}}\n'
		out+=json.dumps(obj)
		docid=docid+1
	return out


def submit_lines(hosts,index_name,index_schema,lines,headers,offset): 
	req=None
	out=prepare_data(index_name, index_schema, lines, headers, offset)
	host=""
	for i in range(10):
		try:
			hostnr = (offset*61) % (len(hosts)) 
			host = hosts[hostnr]
			req = requests.post(host, out)
			if (req.status_code!=200):
				print ("Can't upload data to "+host+"."+req.text+". Will retry...")
				raise Exception("Can't upload data to "+host,req.text)
			return 0
		except Exception:
			if (i<10):
				continue
			else:
				raise
	
	return len(lines)

task_queue=queue.Queue(20)

num_worker_threads=20

pool = multiprocessing.Pool(10)

def worker():
	while True:
		item = task_queue.get()
		pool.apply(submit_lines,item)
		task_queue.task_done()

def start_workers():
	for i in range(num_worker_threads):
		t = threading.Thread(target=worker)
		t.daemon = True
		t.start()
	



def main(argv):
	if (len(argv)<3):
		print ("Usage: "+argv[0]+" input_file index_name index_schema batch_size http://esnode1:port [http://esnode2:port] ...")
		print ("\tinput_file - normal path or - for stdin")
		return
	input_file = argv[1]
	i=0
	batch=1
	lines=[]
	index_name = argv[2]
	index_schema = argv[3]
	batch_size = int(argv[4])
	hosts = argv[5:]
	hosts = [host+"/_bulk" for host in hosts]
	headers=[]
	print ("Using elastic search hosts: "+",".join(hosts))
	host=""

	start_workers()
	start_time=time.time()
	

	docs_count = 0
	with open(input_file, "r") as csvfile:
		headers = csvfile.readline().split(",")
		while True:
			lines=csvfile.readlines(batch_size)
			if (len(lines)==0):
				break
			batch=batch+1
			print(batch)
			#submit_lines(hosts,index_name,index_schema,lines)
			task_queue.put([hosts,index_name,index_schema,lines,headers,docs_count])
			docs_count = docs_count + len(lines)
	task_queue.join()
	pool.close()
	pool.join()
	elapsed_time = time.time()-start_time
	print ("Submited "+str(lines)+" documents with speed of "+str(docs_count/elapsed_time)+"docs/s.")

if __name__ == "__main__":
	main(sys.argv)
