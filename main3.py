import pandas as pd 
from googleplaces import GooglePlaces, types, lang, GooglePlacesError
#from pandas import *
from collections import defaultdict
import time 
import queue
from threading import Thread, Event, Condition, Lock
from fuchsclan import googThread


# lock to serialize console output
#lock = threading.Lock()

que = queue.Queue()
worker_q = queue.Queue()
result_q = queue.Queue()
threads_list = list()

mutex = Lock()
req_mutex = Lock()



#YOUR_API_KEY = 'AIzaSyCxxraww0nAVV2Emm_1HbIFnmhTi3UhZzQ'



MAX_THREADS = 50
XLS_File = "./Arztverzeichnis-work-gesammt"



def read_Exel():
    print ('reading Excel input file')
    #xl = pd.ExcelFile("\\\\sv022181\\abl$\\abl\\Austausch\\Gerald\\Vertragspartner\\Arztverzeichnis-work-gesammt00.xlsx")
    xl = pd.ExcelFile(XLS_File + '.xlsx')
    
    df = xl.parse("Arztverzeichnis 01.01.2018")
    xl.close()
   
    print ('running {} programms in Parallel, handling  {} searches. '.format (MAX_THREADS, len(df)))
    resultDataFrames, remainingDataFrames= traverse(df)
 
 
    
    if len(resultDataFrames)>0:
        print ('writing result excel file')
        rdf= pd.concat(resultDataFrames)   
        writer = pd.ExcelWriter(XLS_File + '-google.xlsx')
        #rdf.to_csv(XLS_File + '-google.vsv' ,index=False, sep='\t', encoding='utf-8')
        rdf.to_excel(writer, sheet_name='Sheet1',index=False)
        writer.save() 

    if len(remainingDataFrames)> 0:
        print ('writing remaining Excel file')
        remdf= pd.concat(remainingDataFrames) 
        writer = pd.ExcelWriter(XLS_File + '-remaining.xlsx')
        #remdf.to_csv(XLS_File + '-remaining.csv',index=False,  sep='\t', encoding='utf-8')
        remdf.to_excel(writer, sheet_name='Sheet1',index=False)
        writer.save() 
        
    
    

def traverse(df):    
    cols = d = defaultdict(list)

    # fill the que wiht the work wich needs to be performed
    for index, row in df.iterrows():
        worker_q.put(df.iloc[[index]])

    df_target = df

    # Create and start ther threads to do the work in the que
    for i in range (MAX_THREADS):
        #t = threading.Thread(target=worker)
        t= googThread.workerThread(i,worker_q,result_q, threads_list, mutex, req_mutex);
        #t = Thread(target=lambda q, i, wq, rq: q.put(googThread.workerThread (i,wq,rq)), args=(que,i, worker_q, result_q,))
        t.start()
        threads_list.append(t)

    for t in threads_list:
        t.join()

    remainingFrames = []
    while not worker_q.empty():
        row = worker_q.get()
        remainingFrames.append(row)


    resultDataFrames = []
    while not result_q.empty():
        resultdf = result_q.get()
        resultDataFrames.append(resultdf)
    return resultDataFrames, remainingFrames
        

e = Event()
start = time.time()
read_Exel()
done = time.time()
elapsed = done - start
print('Ausfuhrungsdauer: {} Sekunden'.format (elapsed))
