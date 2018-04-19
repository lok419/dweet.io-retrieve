import json
import urllib.request
from time import sleep
import sqlite3
from multiprocessing import Pool

dweet_thing1 = "IDP_Group5_1"
dweet_thing2 = "IDP_Group5_2"
db_name = "IDP_Group5.db"

def get_latest_dweet(dweet_thing):
    urlData = "https://dweet.io:443/get/latest/dweet/for/{}".format(dweet_thing)
    webURL = urllib.request.urlopen(urlData)
    data = webURL.read()
    encoding = webURL.info().get_content_charset('utf-8')
    return json.loads(data.decode(encoding))['with'][0]

def init_db():
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("DROP TABLE solar_power")
    c.execute("DROP TABLE wind_power")
    c.execute("DROP TABLE hydro_power")
    c.execute("DROP TABLE environment")
    c.execute("CREATE TABLE solar_power (Date text unique, voltage real, current real, power real, energy real)")
    c.execute("CREATE TABLE wind_power (Date text unique, voltage real, current real, power real, energy real)")
    c.execute("CREATE TABLE hydro_power (Date text unique, voltage real, current real, power real, energy real)")
    c.execute("CREATE TABLE environment (Date text unique, temperature real, humidity real, moisture real, fire INT )")
    conn.commit()
    conn.close()

def write_dweet(json):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    try:
        # insert hydro power data to database
        c.execute('''INSERT INTO hydro_power VALUES (?,?,?,?,?)''', (json[0]['created'],json[0]['content']['hydro_v'],json[0]['content']['hydro_i']
                                                                     ,json[0]['content']['hydro_p'],json[0]['content']['hydro_e']))
        # insert wind power to database
        c.execute('''INSERT INTO wind_power VALUES (?,?,?,?,?)''', (json[0]['created'],json[0]['content']['wind_v'],json[0]['content']['wind_i']
                                                                    ,json[0]['content']['wind_p'],json[0]['content']['wind_e']))
        print("Retreived hydro and wind data at {}".format(json[0]['created']))
    except sqlite3.IntegrityError:
        print("Retreived data already exixts in database")

    try:
        # insert pv power data to database
        c.execute('''INSERT INTO solar_power VALUES (?,?,?,?,?)''', (json[1]['created'],json[1]['content']['solar_v'],json[1]['content']['solar_i']
                                                                     ,json[1]['content']['solar_p'],json[1]['content']['solar_e']))

        c.execute('''INSERT INTO environment VALUES (?,?,?,?,?)''', (json[1]['created'],json[1]['content']['temperature'],json[1]['content']['humidity']
                                                                     ,json[1]['content']['moisture'], 1-json[1]['content']['fire']))
        print("Retreived solar data at {}".format(json[1]['created']))
    except sqlite3.IntegrityError:
        print("Retreived data already exixts in database")

    conn.commit()
    conn.close()

def loop():
    pool = Pool(processes=2)
    while True:
        try:
            last_json = pool.map(get_latest_dweet,[dweet_thing1,dweet_thing2])
            write_dweet(last_json)
        except:
            print('Rate limit exceeded, try again in 1 second(s).')
            sleep(1)

if __name__ == "__main__":
    init_db()
    loop()