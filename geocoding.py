

import urllib2
import json
import db_class_general, sqlite3

def get_latlng(add,type=''):
    DB = db_class_general.DBManager(sqlite3.connect('/path/to/db'))	#Optional external DB to access cached Geocoded information. If nto available use Geocoding API
    res = DB.other_sqlite_ops('select * from Geocode where Place="%s" COLLATE NOCASE'%(add))
    if not res:
        address = urllib2.quote(add)
        geocode_url = "http://maps.googleapis.com/maps/api/geocode/json?address=%s&sensor=false" % address
        print geocode_url
        req = urllib2.urlopen(geocode_url)
        jsonResponse = json.loads(req.read())
        if jsonResponse['status'] == 'ZERO_RESULTS' or not jsonResponse['results']:
            return False
        else:
            latlng = (jsonResponse['results'][0]['geometry']['location']['lat'],jsonResponse['results'][0]['geometry']['location']['lng'])
            try:
                DB.insert_record('Geocode',Place = add, LatLng = ','.join([str(z) for z in latlng]),Place_Type = type)
            except sqlite3.IntegrityError:
                pass
            return latlng

    else:
        return tuple([float(z) for z in res[0][1].split(',')])



