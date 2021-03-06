
import feedparser,os, db_class_general,string,json
from time import mktime
import datetime,sqlite3,requests,urllib,urlparse
import Lookup_GeoID, subprocess
from aggregator_rest import Alchemy
import webbrowser
from geocoding import get_latlng
from  itertools import chain
ref_table = '<table_name>'            #Reference table structure for all subsequently created tables. Create reference table manually or via command line. All subsequent tables will copy this schema
home_path = os.getenv('HOME') + '/'
DB_mgr_feed = db_class_general.DBManager(sqlite3.connect(home_path+'/path/to/db'))   #Sote all data and metadata in Database using db_class_general


def update_feed(rss_url,modified):
    feed = feedparser.parse(rss_url,modified)            
    modified_new = feed.get('modified',feed.get('updated',feed.entries[0].get('published','no modified,update or published fields present in rss')))
    return (feed,modified_new)

def get_site_title_from_URL(url,query_flag=True):
    netloc = urlparse.urlparse(url).netloc
    if not netloc:
        netloc  = urlparse.urlparse(raw_input('\n\nPlease enter full URL : ')).netloc
    cmd = 'select * from Site_Name_Lookup_Cache where Netloc="%s"' %netloc
    results = DB_mgr_feed.other_sqlite_ops(cmd=cmd)
    if results:
        return results[0]

    #Using Google Custom Search API. But limits it to 100 queries a day. Used for getting info on a particular website
    api_key = ''				#Add own api_key and search engine id
    search_engine_id  = ''
    baseurl = 'https://www.googleapis.com/customsearch/v1?'
    if query_flag:
        response = requests.get(baseurl+urllib.urlencode({'key':api_key,'cx':search_engine_id,'q':'info:'+netloc}))
        if response.status_code == 200:
            DB_mgr_feed.insert_record(table_name='Site_Name_Lookup_Cache',Link=netloc, Organisation_Name = response.json()['items'][0]['title'], Description=response.json()['items'][0]['snippet'])
            return    response.json()['items'][0]['title']

    else:
        return False


def analyze_links(url,sentiment = 0):
    Alc = Alchemy()
    entities = Alc.entity_extraction(url = '',sentiment_flag=sentiment)
    return Alc.process_response(response = entities, url = '',sentiment_flag = sentiment)

def analyze_links_retrospectively(table_name,sentiment=0,start_id=None):
    Alc = Alchemy()
    if start_id:
        from_id = start_id
    else:
        from_id = 1
    res = DB_mgr_feed.other_sqlite_ops("select rowid,Link from %s where ifnull(Entities, '') = '' and rowid>%d"%(table_name,from_id,))
    #links = [item[1] for item in res]
    #rowids = [item[0] for item in res]
    for x in res:
        entities = Alc.entity_extraction(url = x[1],sentiment_flag=sentiment)
        if entities:                                                                                            #Check for NoneType
            values = Alc.process_response(response = entities, url = x[1],sentiment_flag = sentiment)
        else:
            with open('Lookup_errors.txt','a+') as f2:
                print >>f2, '\n\n',datetime.datetime.now().strftime('%Y-%m-%d : %H:%M:%S'),'\nEmpty response received of type :', type(entities), '\nURL : ', x[1]
            continue

        entity_names= ' , '.join([item[0] for item in values['sorted_entities']])
        types =  ' , '.join([item[1] for item in values['sorted_entities']])
        relevance =  ' , '.join([item[2] for item in values['sorted_entities']])
        if values['sentiment']:
            sentiment_value =  ' , '.join([str(z) for z in [item[3] for item in values['sorted_entities']]])
        else:
            sentiment_value = ''
        DB_mgr_feed.update_record(table_name,rowid = x[0],Entities = entity_names, Entity_types = types, Relevance = relevance, Sentiment = sentiment_value)

    return


def get_location_from_DB(table_name,start_id=None,end_id=None):

    try:
        if not start_id and not end_id:
            print '\nEither Start or End ID required'
            quit()
        elif start_id and end_id:
            op = DB_mgr_feed.other_sqlite_ops('select Title,Link,Location,Entities,Entity_types,Relevance,Sentiment from %s where rowid>=%d and rowid<%d' %(table_name,start_id,end_id))
            for item in op:
                if any(v is None or v is u'' for v in item[3:]):
                    op.remove(item)

            Title,Link,location,names,types =  [item[0] for item in op],[item[1] for item in op], [item[2] for item in op], [item[3] for item in op], [item[4] for item in op]
            Relevance,Sentiment = [item[5] for item in op],[item[6] for item in op]

        elif start_id and not end_id:
            op = DB_mgr_feed.other_sqlite_ops('select Title,Link,Location,Entities,Entity_types,Relevance,Sentiment from %s where rowid=%d' %(table_name,start_id))
            Title,Link,location,names,types,Relevance,Sentiment = op[0][:]

        elif end_id and not start_id:
            op = DB_mgr_feed.other_sqlite_ops('select Title,Link,Location,Entities,Entity_types,Relevance,Sentiment from %s where rowid=%d' %(table_name,end_id))
            Title,Link,location,names,types,Relevance,Sentiment = op[0][:]


    except IndexError:
        print 'Input Row ID has 0 entities. Select another Row ID'
        quit()



######################################### GENERATING AND FORMATTING JSON file FOR GMAP JAVASCRIPT code ####################################

    location_strings = ['City','Country']
    if len(Title) == len(location) == len(Link):
        num_of_articles = len(Title)
                        # "Object for each article
    article_template = {"Title":None,'Location':None,'lat':None,'lng':None,'Link':None,'City':[],'Country':[]}
    article_list = []
    places_template  = {'Name':None,'lat':None,'lng':None,'Relevance':None,'Sentiment':None}        #For Cities and Countries template

    for i in range(num_of_articles):
        article_template['Title'] = Title[i]
        article_template['Location'] = location[i]
        article_template['Link'] = Link[i]
        article_coord =  get_latlng(location[i])
        article_template['lat']  =article_coord[0]
        article_template['lng']  =article_coord[1]

        for item in location_strings:                                                                   #Loop through city and country keys in DICT
            indices = [k for k,x in enumerate(types[i].split(' , ')) if x == item]
            place_names = [names[i].split(' , ')[j].title() for j in indices]
            relevance_list = [Relevance[i].split(' , ')[j] for j in indices]
            sentiment_list = [Sentiment[i].split(' , ')[j] for j in indices]
            for ind,place in enumerate(place_names):                                                    #Loop through all cities and countries in place_names
                lat,lng = get_latlng(place,type=item)
                article_template[item].append(dict(zip(["Name",'lat','lng','Relevance','Sentiment'],[place,lat,lng,relevance_list[ind],sentiment_list[ind]])))

        article_list.append(article_template)
        article_template = {"Title":None,'Location':None,'lat':None,'lng':None,'Link':None,'City':[],'Country':[]}
    with open(home_path + 'PycharmProjects/Aggregator/JS_Files/data.json','w') as outputfile:
            json.dump({"content":article_list},outputfile)






def main():

#Two sources - Google News and Google Alerts
    Google_Alerts_url = ''           #Create custom alert for a particlar news to track
    Google_news_url = 'https://www.google.com/news?hl=en&gl=in&tbm=nws&as_drrb=q&as_qdr=a&q=<your search term>&oq=ebola&num=100&output=rss'	#Edit 'q' parameter to track a particular search term
    choice = raw_input('\nPress "1" for Google Alerts.\nPress "2" for Google News\nChoice : \t')
    if choice =='1':
        print 'Using Google Alerts API...\n\n\n\n\n\n\n\n\n\n\n'
        url = Google_Alerts_url
    elif choice =='2':
        print 'Using Google News...\n\n\n\n\n\n\n\n\n\n\n'
        url = Google_news_url
    else:
        print 'Invalid Choice :/'
        quit()

    feed = feedparser.parse(url)
    try:
        with open('last_updated_feed.txt','r') as f1:
            modified = f1.read()
    except IOError:
        print '\t\t\t "Last updated" file not found. Using latest feed result...'
        modified = feed.get('modified',feed.get('updated',feed.entries[0].get('published','no modified,update or published fields present in rss')))


    res = update_feed(url,modified)                                         #Update feed at start
    feed = res[0]
    modified_new = res[1]
    with open('last_updated_feed.txt','w') as f1:
        print >> f1, modified_new
    tables = DB_mgr_feed.get_tables_list()
    Entities,Relevance,Sentiment,Entity_types = [],[],[], []
    if feed.status == 200 or feed.status==301:
        search_term = ''
        if url == Google_Alerts_url:
            search_term = feed.feed.title.split(' - ')[-1]                                  #Gives name of Google alert search term
        elif url == Google_news_url:
            search_term =  urlparse.parse_qs(url)['q'][0].lower()
        else:
            print '\t\t\tInvalid source. Source must be Google news nor Google Alerts.'
            quit()
        if search_term.lower() not in [item.lower() for item in tables]:                #Create table for each alert
            DB_mgr_feed.create_table_with_structure(search_term,ref_table)

        exist_in_db_count = 0
        for item in feed['entries']:
            redirect_link = item.links[0].href
            article_link = urlparse.parse_qs(redirect_link)['url'][0]

            title = item.title
            title = title.replace('<b>','')
            title = title.replace('</b>','')

            timestamp =  datetime.datetime.fromtimestamp(mktime(item.updated_parsed))
            current_time = datetime.datetime.now()
            time_str = (timestamp + datetime.timedelta(hours='<offset>')).strftime('%Y-%m-%d : %H:%M:%S') #Offset to change GMT to any time zone
            current_time_str = current_time.strftime('%Y-%m-%d : %H:%M:%S')
            country = Lookup_GeoID.get_country(hostname=article_link)
            Site_name = get_site_title_from_URL(article_link,query_flag=False)                                       #Use query_flag  to skip the custom search api to cut down on usage
            if not Site_name:
                Site_name = ''
            print 'Title :\t',title, 'URL/Link :\t',article_link, '\n\n\n'

            #When analyze_links() is not called!
            if not Entities:
                Entities = ''
            if not Relevance:
                Relevance = ''
            if not Sentiment:
                 Sentiment = ''
            if not Entity_types:
                Entity_types = ''
            ####################################

            try:
                DB_mgr_feed.insert_record(search_term,Timestamp=time_str,Title=title,Link = article_link,Location = country,Organization = Site_name,Id = item.id, Entities = Entities, Entity_types = Entity_types,Relevance = Relevance, Sentiment = Sentiment,Database_entry_time = current_time_str)
            except sqlite3.IntegrityError as err:
                if 'not unique' in err.message:
                    exist_in_db_count +=1
                    continue

                else:
                    print err.message, 
                    quit()
        subprocess.Popen(['notify-send','%d link(s) already exist in the Database'%exist_in_db_count] )

    else:
        print  'HTTP Error returned'

#main()
#analyze_links_retrospectively(table_name='<table>',sentiment=1,start_id=1)

#res = get_location_from_DB('ebola',start_id=250,end_id=255)
#import Launch_Server

#if not res:
#    print 'The article did not contain any places'
#    quit()




