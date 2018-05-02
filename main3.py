import pandas as pd 
from googleplaces import GooglePlaces, types, lang, GooglePlacesError
#from pandas import *
from collections import defaultdict
import time 
import queue
from threading import Thread, Event
from fuchsclan import googThread


# lock to serialize console output
#lock = threading.Lock()

que = queue.Queue()
worker_q = queue.Queue()
result_q = queue.Queue()
threads_list = list()



#YOUR_API_KEY = 'AIzaSyCxxraww0nAVV2Emm_1HbIFnmhTi3UhZzQ'



MAX_THREADS = 5
XLS_File = "./Arztverzeichnis-work-5000"



def read_Exel():
    #xl = pd.ExcelFile("\\\\sv022181\\abl$\\abl\\Austausch\\Gerald\\Vertragspartner\\Arztverzeichnis-work-gesammt00.xlsx")
    xl = pd.ExcelFile(XLS_File + '.xlsx')
    
    df = xl.parse("Arztverzeichnis 01.01.2018")
    xl.close()
   
    print ('running {} programms in Parallel, handling  {} searches. '.format (MAX_THREADS, len(df)))

    resultDataFrames = traverse(df)
    rdf= pd.concat(resultDataFrames)    
    #target = traverse(df)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    #writer = pd.ExcelWriter('\\\\sv022181\\abl$\\abl\\Austausch\\Gerald\\Vertragspartner\\Arztverzeichnis-work-google.xlsx')   
    #target = traverse(df)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    #writer = pd.ExcelWriter('\\\\sv022181\\abl$\\abl\\Austausch\\Gerald\\Vertragspartner\\Arztverzeichnis-work-google.xlsx')
    
    
    writer = pd.ExcelWriter(XLS_File + '-google.xlsx')
    rdf.to_excel(writer, sheet_name='Sheet1')
    writer.save()

def traverse(df):    
    cols = d = defaultdict(list)
    
    df_target = df
    for i in range (MAX_THREADS):
        #t = threading.Thread(target=worker)
        t= googThread.workerThread(i,worker_q,result_q);
        #t = Thread(target=lambda q, i, wq, rq: q.put(googThread.workerThread (i,wq,rq)), args=(que,i, worker_q, result_q,))
        t.daemon=True
        t.start()
        threads_list.append(t)

    
    for index, row in df.iterrows():
        worker_q.put(df.iloc[[index]])
   
    # Wait until que is empty again. 
    worker_q.join()

    #Kill each thread
    for i in range (MAX_THREADS):
        worker_q.put(None)

    # wait untill all threads are killed
    for t in threads_list:
        t.join()


    resultDataFrames = []
    while not result_q.empty():
        resultdf = result_q.get()
        resultDataFrames.append(resultdf)
    return resultDataFrames
        

e = Event()
start = time.time()
read_Exel()
done = time.time()
elapsed = done - start
print('Ausfuhrungsdauer: {} Sekunden'.format (elapsed))
