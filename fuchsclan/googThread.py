from threading import Thread, Event
from googleplaces import GooglePlaces, types, lang, GooglePlacesError
import pandas as pd 
import time 
import urllib.request, urllib.error
from decimal import Decimal
import json

#YOUR_API_KEY = 'AIzaSyD3wdGHiewoTV3iAGadKK7WkCLwqdhjyJs'
YOUR_API_KEY = 'AIzaSyCOXwbiVzqW8j9khZd12tK9CzEANmaygys'
MAX_RETRIES = 1
ERASE_LINE = '\x1b[2K'

BASE_URL = 'https://maps.googleapis.com/maps/api'
PLACE_URL = BASE_URL + '/place'
TEXT_SEARCH_API_URL = PLACE_URL + '/textsearch/json?'
DETAIL_API_URL = PLACE_URL + '/details/json?'

RESPONSE_STATUS_OK = 'OK'
RESPONSE_STATUS_ZERO_RESULTS = 'ZERO_RESULTS'



class workerThread (Thread):
    def __init__(self, threadID, worker_q, result_q, threadsList, mutex, req_mutex):
        Thread.__init__(self)
        self.threadID = threadID
        self.worker_q = worker_q
        self.result_q = result_q
        self.google_places = GooglePlaces(YOUR_API_KEY)
        self.threadsList = threadsList
        self.mutex = mutex
        self.req_mutex = req_mutex
    
    def run(self):
        self.worker(self.worker_q, self.result_q,self.threadID)
        
    def getPlacesPLZ(self,place):
        for ac in place:
            if 'postal_code' in ac['types']:
                return ac['long_name']
        return ''

    def getPlacesHN(self,place):
        for ac in place:
            if 'street_number' in ac['types']:
            #if ac['types'][0] == 'street_number':
                return ac['long_name']
        return ''

    def getPlacesStrasse(self,place):
        for ac in place:
            if 'route' in ac['types']:
            #if ac['types'][0] == 'route':
                return ac['long_name']
        return ''

    def getPlacesOrt(self,place):
        for ac in place:
            if 'locality' in ac['types']:
            #if ac['types'][0] == 'locality':
                return ac['long_name']
        return ''


    def worker(self,worker_q, result_q, threadnr):
        
        while True:
            
            if not worker_q.empty():
                row = worker_q.get()
            else:
                break
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
            self.req_mutex.acquire()
            try:
                s_result = self.do_google_search(searchstr)
            except:
                worker_q.put(row)
                self.req_mutex.release()
                break
            else:
                self.newmethod986(s_result, row, threadnr)
                self.worker_q.task_done()
                self.req_mutex.release()  

    def newmethod986(self,s_result, row,thread):
        if len(s_result.places) == 0:
            # nothing found, try it without "Ort"
            searchstr = row['Titel'].item()+' '+row['Vorname'].item()+' '+row['Nachname'].item()+', '
            s_result = self.do_google_search(searchstr)

        i = 0
        while i < len(s_result.places):
            place = s_result.places[i]
        #for idx, place in enumerate(s_result.places):
            idx = i
            #print ('{} verarbeite Zeile {} {}'.format(time.strftime("%d.%m.%Y %H:%M:%S"),row.index.item()+1,place.name))
            print ('\033[{};0H\x1b[2K{}-thread {} verarbeitet Zeile {} {}'.format(thread,time.strftime("%d.%m.%Y %H:%M:%S"),thread+1,row.index.item()+1,place.name))
            if row.index.item() >= 4990:
                print('last Record')

            try:    
                place = self.get_place_detail(place)
            except:
                add_empty_google_result()
            else:
                plz = self.getPlacesPLZ(place.details['address_components'])
                ort = self.getPlacesOrt(place.details['address_components'])
                hausnummer = self.getPlacesHN(place.details['address_components'])
                strasse = self.getPlacesStrasse(place.details['address_components'])

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
                self.result_q.put(rdf)
            i = i+1

    def get_place_detail(self,place):
        for loop in range(MAX_RETRIES):
            try:
                place.get_details()
                return place
            except GooglePlacesError as error_detail:
                if str(error_detail).find('OVER_QUERY_LIMIT') >= 0:
                    print ('\033[{};0H\x1b[2K{}-thread {} going to sleep for an hour'.format(self.threadID,time.strftime("%d.%m.%Y %H:%M:%S"),self.threadID+1))

                    time.sleep(900)
                    print ('\033[{};0H\x1b[2K{}-thread {} return from sleep in details'.format(self.threadID,time.strftime("%d.%m.%Y %H:%M:%S"),self.threadID+1))
                    
                    # retry after sleep
                else:
                    print (error_detail)
                    # retry emediataly
        raise Exception

        

    def add_empty_google_result(self,df,result_q):
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
   

    def do_google_search(self,searchstring):
        for loop in range(MAX_RETRIES):
            try:
                ret = self.google_places.text_search(searchstring, language='de', radius=20000)
               
               
                #successfull leave loop
                return ret

            except GooglePlacesError as error_detail:
                if str(error_detail).find('OVER_QUERY_LIMIT') >= 0:
                    pass
                else:
                    print (error_detail)
                    # retry emediataly

        # MAX_RETRIES reached raise an exception
        raise Exception

    def find_and_remove(self, x):
        self.mutex.acquire()
        for e in self.threadsList:
            if e.ident == x:
                self.threadsList.remove(e)
                self.mutex.release()
                return True
        self.mutex.release()
        return False
                    
    
   