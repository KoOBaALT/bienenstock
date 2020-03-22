import geocoder

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Float

import pytz
from datetime import datetime

from geopy.distance import distance
import geocoder

from flask import Flask, jsonify, request

import json


def get_lat_lon (place_name):

        # get geocode postion
        g = geocoder.osm(place_name)
       
        if g.ok == True:
            lat = g.json ["lat"]
            lon = g.json ["lng"]
            
        else:
            lat = 0.0
            lon = 0.0
            print("No LatLon")
            self.is_error[0] = 1
            self.error_info = self.error_info + " " + 'Place ' + place_name + ' not found'

        return(str(lat),str(lon))


app = Flask(__name__)

TABLE_NAME = "tabel_data_master_3"


###### DIALOG FLOW SITE ########

@app.route('/api/new_user/')
def new_user():
    vorname = request.args.get('vorname')
    nachname = request.args.get('nachname')
    adresse = request.args.get('adresse')
    phone = request.args.get('phone')
    req_user_id = request.args.get('req_user_id')
    
    vorname = vorname.replace("%20", " ")
    nachname = nachname.replace("%20", " ")
    adresse = adresse.replace("%20", " ")
    phone = phone.replace("%20", " ")
    req_user_id = req_user_id.replace("%20", " ")
    
    
    lat, lon = get_lat_lon(adresse)
    
    tz = pytz.timezone('Europe/Berlin')
    datetime_now = str(datetime.now(tz))
    
    engine = create_engine('mysql+mysqlconnector://gerda_api:Dr.$nvnGa1+Mt@localhost/gerda_db')

    sql = """UPDATE """+TABLE_NAME+"""
    SET 
    req_vorname ='"""+vorname+"""',
    req_nachname ='"""+nachname+"""',
    req_adresse ='"""+adresse+"""',
    req_lat ="""+lat+""",
    req_lon ="""+lon+""",
    req_phone_nummber ='"""+phone+"""',
    datetime ='"""+ datetime_now + """',
    status = 'Suche Helfer'
    WHERE req_user_id = '""" + req_user_id + """';"""

    with engine.begin() as conn:    
        conn.execute(sql)
    
    engine.dispose()
    
    return str(True)


@app.route('/api/category_text/')
def category_text():
    
    req_user_id = request.args.get('req_user_id')
    req_category_text = request.args.get('req_category_text')
    req_category = request.args.get('req_category')
    
    req_category_text = req_category_text.replace("%20", " ")
    req_category = req_category.replace("%20", " ")
    
    req_user_id = req_user_id.replace("%20", " ")
    
    lat = 54.001241
    lon = 43.001215
    
    col_names = ["req_phone_nummber", "req_user_id", "req_vorname", "req_nachname", "req_adresse","req_lat","req_lon", "req_category","req_category_text", "hero_phone", "hero_user_id", "hero_vorname", "hero_nachname", "hero_adresse", "status", "datetime", "task"]
    values = ["nan", req_user_id, "nan", "nan", "nan",lat,lon, req_category,req_category_text, "nan", "nan", "nan", "nan", "nan", "Auftrag angelegt", "nan", "nan"]

    df = pd.DataFrame([values], columns = col_names)
    
    engine = create_engine('mysql+mysqlconnector://gerda_api:Dr.$nvnGa1+Mt@localhost/gerda_db')
    df.to_sql(con=engine, name=TABLE_NAME, if_exists='append', dtype = {"req_lat": Float(precision=6, asdecimal = True, decimal_return_scale =10),"req_lon": Float(precision=6, asdecimal = True, decimal_return_scale= 10)} )
    engine.dispose()

    
    return str(True)


@app.route('/api/new_user_request/')
def new_user_request():
    vorname = request.args.get('vorname')
    nachname = request.args.get('nachname')
    adresse = request.args.get('adresse')
    phone = request.args.get('phone')
    req_category_text = request.args.get('req_category_text')
    req_category = request.args.get('req_category')
    req_user_id = request.args.get('req_user_id')
    

    vorname = vorname.replace("%20", " ")
    nachname = nachname.replace("%20", " ")
    adresse = adresse.replace("%20", " ")
    phone = phone.replace("%20", " ")
    req_category_text = req_category_text.replace("%20", " ")
    req_category = req_category.replace("%20", " ")
    req_user_id = req_user_id.replace("%20", " ")
    
    
    lat, lon = get_lat_lon(adresse)

    tz = pytz.timezone('Europe/Berlin')
    datetime_now = str(datetime.now(tz))
    
    col_names = ["req_phone_nummber", "req_user_id", "req_vorname", "req_nachname", "req_adresse","req_lat","req_lon", "req_category","req_category_text", "hero_phone", "hero_user_id", "hero_vorname", "hero_nachname", "hero_adresse", "status", "datetime", "task"]
    values = [phone, req_user_id, vorname, nachname, adresse,lat,lon, req_category,req_category_text, "nan", "nan", "nan", "nan", "nan", "Suche Helfer", datetime_now, "nan"]

    df = pd.DataFrame([values], columns = col_names)
    engine = create_engine('mysql+mysqlconnector://gerda_api:Dr.$nvnGa1+Mt@localhost/gerda_db')
    df.to_sql(con=engine, name=TABLE_NAME, if_exists='append', dtype = {"req_lat": Float(precision=6, asdecimal = True, decimal_return_scale =10),"req_lon": Float(precision=6, asdecimal = True, decimal_return_scale= 10)} )
    engine.dispose()
    
    return str(True)

###### APP SITE ########

@app.route('/api/new_job/')
def new_job():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    radius = request.args.get('radius')
    
    #0.005 guter Wert
    lat_max = str(float(lat) + float(radius))
    lat_min = str(float(lat) - float(radius))
    lon_max = str(float(lon) + float(radius))
    lon_min = str(float(lon) - float(radius))
    
    engine = create_engine('mysql+mysqlconnector://gerda_api:Dr.$nvnGa1+Mt@localhost/gerda_db')

    df = pd.read_sql(sql="SELECT req_user_id,req_vorname,req_lat,req_lon,req_category,req_category_text  FROM "+TABLE_NAME+" WHERE req_lat<"+lat_max+" AND req_lat>"+lat_min+" AND req_lon<"+lon_max+" AND req_lon>"+lon_min+"AND status='Suche Helfer'", con=engine)
    
    engine.dispose()

    return df.to_json(orient = "records")


@app.route('/api/abort_job/')
def abort_job():
    req_user_id = request.args.get('req_user_id')
    req_category  = request.args.get('task')
    engine = create_engine('mysql+mysqlconnector://gerda_api:Dr.$nvnGa1+Mt@localhost/gerda_db')

    sql = """UPDATE """+TABLE_NAME+"""
    SET 
    hero_phone ='""" + "nan" + """',
    hero_user_id ='""" + "nan" + """',
    hero_vorname ='""" + "nan" + """',
    hero_nachname ='""" + "nan" + """',
    hero_adresse ='""" + "nan" + """',
    status ='Suche Helfer'
    WHERE req_user_id = '""" + req_user_id + """' AND req_category = '"""+req_category+"""' ;"""
    
    engine.dispose()

    return str(True)


@app.route('/api/job_accept/')
def job_accept():
    req_user_id = request.args.get('req_user_id')
    hero_phone = request.args.get('hero_phone')
    hero_user_id = request.args.get('hero_user_id')
    hero_vorname = request.args.get('hero_vorname')
    hero_nachname = request.args.get('hero_nachname')
    hero_adresse = request.args.get('hero_adresse')

    
    hero_phone = hero_phone.replace("_", " ")
    hero_user_id = hero_user_id.replace("_", " ")
    hero_vorname = hero_vorname.replace("_", " ")
    hero_nachname = hero_nachname.replace("_", " ")
    hero_adresse = hero_adresse.replace("_", " ")


    
    sql = """UPDATE """+TABLE_NAME+"""
    SET 
    hero_phone ='""" + hero_phone + """',
    hero_user_id ='""" + hero_user_id + """',
    hero_vorname ='""" + hero_vorname + """',
    hero_nachname ='""" + hero_nachname + """',
    hero_adresse ='""" + hero_adresse + """',
    status ='Auftrag angenommen'
    WHERE req_user_id = '""" + req_user_id + """';"""

    with engine.begin() as conn:    
        conn.execute(sql)


    engine.dispose()

    return df.to_json(orient = "records")


@app.route('/api/job_alive/')
def job_alive():
    req_user_id = request.args.get('req_user_id')
    req_user_id = req_user_id.replace("_", " ")
    
    engine = create_engine('mysql+mysqlconnector://gerda_api:Dr.$nvnGa1+Mt@localhost/gerda_db')
    
    df = pd.read_sql(sql="SELECT * FROM "+ TABLE_NAME +" WHERE req_user_id = '""" + req_user_id + """'""", con=engine)

    engine.dispose()
    
    return str(df.status.values[0] != "Auftrag abgebrochen")
    
@app.route('/api/update_status/')
def update_status():
    req_user_id = request.args.get('req_user_id')
    status = request.args.get('status')
    
    
    req_user_id = req_user_id.replace("_", " ")
    status = status.replace("_", " ")
    
    engine = create_engine('mysql+mysqlconnector://gerda_api:Dr.$nvnGa1+Mt@localhost/gerda_db')
    
    sql = """UPDATE """+TABLE_NAME+"""
    SET 
    status ='"""+status+"""'
    WHERE req_user_id = '""" + req_user_id + """';"""

    with engine.begin() as conn:    
        conn.execute(sql)
        
    engine.dispose()
    
    return str(True)
    
   
    
@app.route('/api/job_finished/')
def job_finished():
    req_user_id = request.args.get('req_user_id')
    req_category = request.args.get('req_category')
    
    req_user_id = req_user_id.replace("_", " ")

    engine = create_engine('mysql+mysqlconnector://gerda_api:Dr.$nvnGa1+Mt@localhost/gerda_db')
    
    sql = "DELETE FROM "+TABLE_NAME+" WHERE req_user_id ='"+req_user_id+"' AND req_category ='"+req_category+"'"

    with engine.begin() as conn:    
        conn.execute(sql)
        
    engine.dispose()
    
    return str(True)

if __name__ == '__main__':
    app.run(host= '0.0.0.0', port = 5001, debug=False)
