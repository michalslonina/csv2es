#!/usr/bin/python
import requests
import sys
import csv
import json
import time
import Queue
import threading
import pdb

def prepare_data(index_name, index_schema, lines, headers):
	try:
		prepare_data.document_counter += 1
	except AttributeError:
		prepare_data.document_counter = 0
	docid=prepare_data.document_counter = 0
	out=""
	for line in lines:
		vals = line.split(",")
		obj = dict(zip(headers,vals))
		out+='{"index":{"_index":"'+index_name+'","_type":"'+index_schema+'","_id":"'+str(docid)+'"}}\n'
		out+=json.dumps(obj)
	return out


def submit_lines(hosts, index_name, index_schema, lines, headers): 
	req=None
	try:
		submit_lines.batch_counter += 1
		submit_lines.docs_counter += len(lines)
	except AttributeError:
		submit_lines.batch_counter = 0
		submit_lines.docs_counter = 0
		submit_lines.start_time = time.time()
	out=prepare_data(index_name, index_schema, lines, headers)
	host=""
	for i in range(10):
		try:
			hostnr = submit_lines.batch_counter % (len(hosts)) 
			host = hosts[hostnr]
			req = requests.post(host, out)
			if (req.status_code!=200):
				print ("Can't upload data to "+host+"."+req.text+". Will retry...")
				raise Exception("Can't upload data to "+host,req.text)
			secs_since_start = time.time() - submit_lines.start_time
			print ("Submited batch "+str(submit_lines.batch_counter)+". Speed "+str(submit_lines.docs_counter/secs_since_start)+" docs/s.")
			return
		except Exception:
			continue
	
	raise Exception("Can't upload data to "+host)

task_queue = Queue.Queue(50)
num_worker_threads=20
def worker():
    while True:
        item = task_queue.get()
        submit_lines(item["hosts"],item["index_name"],item["index_schema"],item["lines"], item["headers"])
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

	with open(input_file, "r") as csvfile:
		headers = csvfile.readline().split(",")
		while True:
			lines=csvfile.readlines(batch_size)
			batch=batch+1
			print(batch)
			#submit_lines(hosts,index_name,index_schema,lines)
			task_queue.put({"hosts":hosts,"index_name":index_name,"index_schema":index_schema,"lines":lines,"headers":headers})
	if (len(lines)>0):
		submit_lines(hosts,index_name,index_schema,lines)
	task_queue.join()

if __name__ == "__main__":
	main(sys.argv)
