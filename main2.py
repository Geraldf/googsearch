import pandas as pd 
from googleplaces import GooglePlaces, types, lang, GooglePlacesError
#from pandas import *
from collections import defaultdict
import time 
import queue
from threading import Thread, Event


# lock to serialize console output
#lock = threading.Lock()

que = queue.Queue()
worker_q = queue.Queue()
result_q = queue.Queue()
threads_list = list()



#YOUR_API_KEY = 'AIzaSyCxxraww0nAVV2Emm_1HbIFnmhTi3UhZzQ'

YOUR_API_KEY = 'AIzaSyD3wdGHiewoTV3iAGadKK7WkCLwqdhjyJs'

MAX_THREADS = 50
MAX_RETRIES = 24

ERASE_LINE = '\x1b[2K'

google_places = GooglePlaces(YOUR_API_KEY)

def read_Exel():
    #xl = pd.ExcelFile("\\\\sv022181\\abl$\\abl\\Austausch\\Gerald\\Vertragspartner\\Arztverzeichnis-work-gesammt00.xlsx")
    xl = pd.ExcelFile("./Arztverzeichnis-work-5000.xlsx")
    
    df = xl.parse("Arztverzeichnis 01.01.2018")
    xl.close()
    print(chr(27) + "[2J")
    print ('running {} programms in Parallel, handling  {} searches. '.format (MAX_THREADS, len(df)))

    resultDataFrames = traverse(df)
    rdf= pd.concat(resultDataFrames)    
    #target = traverse(df)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    #writer = pd.ExcelWriter('\\\\sv022181\\abl$\\abl\\Austausch\\Gerald\\Vertragspartner\\Arztverzeichnis-work-google.xlsx')   
    #target = traverse(df)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    #writer = pd.ExcelWriter('\\\\sv022181\\abl$\\abl\\Austausch\\Gerald\\Vertragspartner\\Arztverzeichnis-work-google.xlsx')
    
    
    writer = pd.ExcelWriter('./Arztverzeichnis-work-5000-google.xlsx')
    rdf.to_excel(writer, sheet_name='Sheet1')
    writer.save()

def traverse(df):    
    cols = d = defaultdict(list)
    
    df_target = df
    for i in range (MAX_THREADS):
        #t = threading.Thread(target=worker)
        t = Thread(target=lambda q, wq, rq,i: q.put(worker(wq,rq,i)), args=(que, worker_q, result_q,i ))
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
        


def getPlacesPLZ(place):
    for ac in place:
        if 'postal_code' in ac['types']:
        #if ac['types'][0] == 'postal_code':
            return ac['long_name']
    return ''

def getPlacesHN(place):
    for ac in place:
        if 'street_number' in ac['types']:
        #if ac['types'][0] == 'street_number':
            return ac['long_name']
    return ''

def getPlacesStrasse(place):
    for ac in place:
        if 'route' in ac['types']:
        #if ac['types'][0] == 'route':
            return ac['long_name']
    return ''

def getPlacesOrt(place):
    for ac in place:
        if 'locality' in ac['types']:
        #if ac['types'][0] == 'locality':
            return ac['long_name']
    return ''


def worker(worker_q, result_q,i):
    print ('starting Thread {}'.format(i))
    while True:
        
        row = worker_q.get()
        
        if row is None:
            # kill this worker thread
            break 

        #row = row.to_frame()
        if pd.isna(row['Titel'].item()):
            row['Titel'] = ''

        if pd.isna(row['Ort'].item()):
            row['Ort'] = ''
            
        #searchstr = "{} {} {}".format(row['Titel'],row['Vorname'],row['Nachname'])
        searchstr = row['Titel'].item()+' '+row['Vorname'].item()+' '+row['Nachname'].item()+', '+row['Ort'].item()
        
        try:


            s_result = do_google_search(searchstr)
        except:
            add_empty_google_result()
        else:
            newmethod986(s_result, row, i)
            worker_q.task_done()

def newmethod986(s_result, row, thread):
    if len(s_result.places) == 0:
        # nothing found, try it without "Ort"
        searchstr = row['Titel'].item()+' '+row['Vorname'].item()+' '+row['Nachname'].item()+', '
        s_result = do_google_search(searchstr)

    i = 0
   
    while i < len(s_result.places):
        place = s_result.places[i]
    #for idx, place in enumerate(s_result.places):
        idx = i
        print ('\033[{};0H\x1b[2K{}-thread {} verarbeitet Zeile {} {}'.format(thread,time.strftime("%d.%m.%Y %H:%M:%S"),thread+1,row.index.item()+1,place.name))

        try:    
            place = get_place_detail(place)
        except:
            add_empty_google_result()
        else:
            plz = getPlacesPLZ(place.details['address_components'])
            ort = getPlacesOrt(place.details['address_components'])
            hausnummer = getPlacesHN(place.details['address_components'])
            strasse = getPlacesStrasse(place.details['address_components'])

            if 'international_phone_number' in place.details:
                phone = place.details['international_phone_number']
            else:
                phone =''

            name = place.name
            if 'website' in place.details:
                web = place.details['website']
            else:
                web =''
            if 'rating' in place.details:
                rating = place.details['rating']
            else:
                rating = ''
            adr_formatiert = place.details['formatted_address']

            if('types' in place.details):
                types = ','.join(place.details['types'])
            else:
                types = ''

            if('url' in place.details):
                url = place.details['url']
            else:
                url = ''

            if pd.isna(row['PLZ'].item()):
                row.loc[0:'PLZ']=0
                #row['PLZ'] = 0
            plzdiff =  abs(int(row['PLZ'].item()) - int(plz))
            rdf = row.copy()     
            rdf['goog_name'] = name
            rdf['goog_plz'] = plz 
            rdf['goog_web'] = web 
            rdf['goog_ort']= ort
            rdf['goog_phone']= phone
            rdf['goog_hausnummer']= hausnummer
            rdf['goog_strasse']= strasse
            rdf['goog_rating']= rating
            rdf['goog_adr_formatiert']= adr_formatiert
            rdf['goog_types']= types    
            rdf['goog_url'] = url
            rdf['goog_plzdiff'] = plzdiff
            result_q.put(rdf)
        i = i+1

def get_place_detail(place):
    for loop in range(MAX_RETRIES):
        try:
            place.get_details()
            return place
        except GooglePlacesError as error_detail:
            if str(error_detail).find('OVER_QUERY_LIMIT') >= 0:
                print ('{} going to sleep for an hour\n'.format(time.strftime("%d.%m.%Y %H:%M:%S")))
                time.sleep(3600)
                # retry after sleep
            else:
                print (err)
                # retry emediataly
    raise Exception

    

def add_empty_google_result(df,result_q):
    rdf = df
    rdf['goog_name'] = ''
    rdf['goog_plz'] = '' 
    rdf['goog_web'] = '' 
    rdf['goog_ort']= ''
    rdf['goog_phone']= ''
    rdf['goog_hausnummer']= ''
    rdf['goog_strasse']= ''
    rdf['goog_rating']= ''
    rdf['goog_adr_formatiert']= ''
    rdf['goog_types']= ''
    rdf['goog_url'] =''
    rdf['goog_plzdiff'] = ''
    result_q.put(rdf)
    return


   




def do_google_search(searchstring):
    for loop in range(MAX_RETRIES):
        try:
            ret = google_places.text_search(searchstring, language='de', radius=20000)
            #successfull leave loop
            return ret

        except GooglePlacesError as error_detail:
            if str(error_detail).find('OVER_QUERY_LIMIT') >= 0:
                print ('{} going to sleep for an hour\n'.format(time.strftime("%d.%m.%Y %H:%M:%S")))
                time.sleep(3600)
                # retry after sleep
            else:
                print (err)
                # retry emediataly

    # MAX_RETRIES reached raise an exception
    raise Exception
                
        
 



        

e = Event()
start = time.time()
read_Exel()
done = time.time()
elapsed = done - start
print('Ausfuhrungsdauer: {} Sekunden'.format (elapsed))
