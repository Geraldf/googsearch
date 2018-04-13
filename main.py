import pandas as pd 
from googleplaces import GooglePlaces, types, lang
#from pandas import *
from collections import defaultdict
import time 
from multiprocessing import Queue
from threading import Thread


# lock to serialize console output
#lock = threading.Lock()
que = Queue()
threads_list = list()


#YOUR_API_KEY = 'AIzaSyCxxraww0nAVV2Emm_1HbIFnmhTi3UhZzQ'

YOUR_API_KEY = 'AIzaSyD3wdGHiewoTV3iAGadKK7WkCLwqdhjyJs'

google_places = GooglePlaces(YOUR_API_KEY)

def read_Exel():
    #xl = pd.ExcelFile("\\\\sv022181\\abl$\\abl\\Austausch\\Gerald\\Vertragspartner\\Arztverzeichnis-work-5000.xlsx")
    xl = pd.ExcelFile("./Arztverzeichnis-work-5000.xlsx")
    
    df = xl.parse("Arztverzeichnis 01.01.2018")
    xl.close()
    max_rows = 100
    dataframes = []
    while len(df) > max_rows:
        top = df[:max_rows]
        top.reset_index(inplace=True, drop=True)
        dataframes.append(top)
        df = df[max_rows:]
    else:
        dataframes.append(df)
    print ('running {} programms in Parallel, each Programm performs {} searches. '.format (len(dataframes), max_rows))
    for f in dataframes:
   
        t = Thread(target=lambda q, arg1: q.put(traverse(arg1)), args=(que, f))
        t.start()
        threads_list.append(t)

    # Join all the threads
    for t in threads_list:
        t.join()

    # Check thread's return value
    resultDataFrames = []
    while not que.empty():
        resultdf = que.get()
        resultDataFrames.append(resultdf)

    rdf= pd.concat(resultDataFrames)    
    #target = traverse(df)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    #writer = pd.ExcelWriter('\\\\sv022181\\abl$\\abl\\Austausch\\Gerald\\Vertragspartner\\Arztverzeichnis-work-gesammt-google.xlsx')
    
    
    writer = pd.ExcelWriter('./Arztverzeichnis-work-5000-google.xlsx')
    rdf.to_excel(writer, sheet_name='Sheet1')
    writer.save()

def traverse(df):
    
    cols = d = defaultdict(list)
    df_target = df
  
    for index, row in df.iterrows():
        df_target = googSearch(row, df, index, cols, df_target)
    df_target['goog_name'] = cols['goog_name']
    df_target['goog_plz'] = cols['goog_plz'] 
    df_target['goog_web'] = cols['goog_web'] 
    df_target['goog_ort']= cols['goog_ort']
    df_target['goog_phone']= cols['goog_phone']
    df_target['goog_hausnummer']= cols['goog_hausnummer']
    df_target['goog_strasse']= cols['goog_strasse']
    df_target['goog_rating']= cols['goog_rating']
    df_target['goog_adr_formatiert']= cols['goog_adr_formatiert']
    df_target['goog_types']= cols['goog_types']
    df_target['goog_url'] =cols['goog_url']
    df_target['goog_plzdiff'] = cols['goog_plzdiff']
    

    return df_target
        


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


def googSearch(row, googdf, index, cols, target_DF):
    if pd.isna(row['Titel']):
        row['Titel'] = ''

    if pd.isna(row['Ort']):
        row['Ort'] = ''
        
    #searchstr = "{} {} {}".format(row['Titel'],row['Vorname'],row['Nachname'])
    searchstr = row['Titel']+' '+row['Vorname']+' '+row['Nachname']+', '+row['Ort']
    s_result = google_places.text_search(searchstr, language='de', radius=20000)

    if len(s_result.places) == 0:
        # nothing found, try it without "Ort"
        searchstr = row['Titel']+' '+row['Vorname']+' '+row['Nachname']+', '
        s_result = google_places.text_search(searchstr, language='de', radius=20000)


    if len(s_result.places) == 0:
        cols['goog_name'].append('')
        cols['goog_plz'].append('') 
        cols['goog_web'].append('') 
        cols['goog_ort'].append('')
        cols['goog_phone'].append('')
        cols['goog_hausnummer'].append('')
        cols['goog_strasse'].append('')
        cols['goog_rating'].append('')
        cols['goog_adr_formatiert'].append('' )
        cols['goog_types'].append('')
        cols['goog_url'].append('')
        cols['goog_plzdiff'].append('')

        return target_DF

    for idx, place in enumerate(s_result.places):

        print ('{} verarbeite Zeile {} {} '.format(time.strftime("%d.%m.%Y %H:%M:%S"),index+1,place.name))
        #print (place.place_id)
        #print ()
        place.get_details()

        plz = getPlacesPLZ(place.details['address_components'])
        ort = getPlacesOrt(place.details['address_components'])
        hausnummer = getPlacesHN(place.details['address_components'])
        strasse = getPlacesStrasse(place.details['address_components'])

        if 'international_phone_number' in place.details:
            phone = place.details['international_phone_number']
        else:
            phone =''
        
        name = place.name
        sizeDiff = len(target_DF.values)- len(googdf.values)

        if idx > 0:
            s1 = googdf.iloc[index]
            s2 = target_DF.iloc[0:index+sizeDiff]
            s3 = target_DF.iloc[index+sizeDiff:]
            s4 = pd.concat([s1.to_frame().T],ignore_index=True)
            target_DF = pd.concat([s2,s4, s3],ignore_index=True)
            
            

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

        if pd.isna(row['PLZ']):
            row['PLZ'] = 0
        plzdiff =  abs(int(row['PLZ']) - int(plz))

        cols['goog_name'].append(name)
        cols['goog_plz'].append(plz) 
        cols['goog_web'].append(web) 
        cols['goog_ort'].append(ort)
        cols['goog_phone'].append(phone)
        cols['goog_hausnummer'].append(hausnummer)
        cols['goog_strasse'].append(strasse)
        cols['goog_rating'].append(rating)
        cols['goog_adr_formatiert'].append(adr_formatiert )
        cols['goog_types'].append(types)
        cols['goog_url'].append(url)
        cols['goog_plzdiff'].append(plzdiff)

    return target_DF


start = time.time()
read_Exel()
done = time.time()
elapsed = done - start
print('Ausführungsdauer: {} Sekunden'.format (elapsed))