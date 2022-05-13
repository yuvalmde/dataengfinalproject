
import requests
import pymongo
import telebot
from requests.structures import CaseInsensitiveDict
import datetime
from datetime import date
#### Defining Constant Variables ####
TOKEN = "5151016014:AAFIwk5NgGNeYHdJNVBmj_pQtQqHcjvtlok"
bot = telebot.TeleBot(TOKEN)
#### Defining MongoDB Constant Variables ####
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Amadeus_DB"]
mycol = mydb["Flights_Requests"]
#### Generating Today's date ####
today = date.today()
current_date = today.strftime("%d_%m_%Y")

def format_time():
    t = datetime.datetime.now()
    s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
    return s[:-3]

new_ts = format_time()
current_timestamp =  datetime.datetime.now()

#### Telegram Bot Message to User When Price Has Changed ####
def send_message_to_user(chat_id,user_first_name,new_price,origin_city, dest_city, dep_date, return_date,original_price_total):
  bot.send_message(chat_id, f'Hi {user_first_name}, Good news! The old price for flight from {origin_city} to {dest_city} between '
                             f'{dep_date} and {return_date} was {original_price_total}$, the new price is {new_price}$' )

#### Amadeus ######
def amadeus_check_price(origin_city, dest_city, dep_date, return_date):
  url = "https://test.api.amadeus.com/v1/security/oauth2/token"
  headers = CaseInsensitiveDict()
  headers["Content-Type"] = "application/x-www-form-urlencoded"
  data = "grant_type=client_credentials&client_id=eMssgdZ69DLAO3jUZLJvircW9yoJrq9s&client_secret=idNOH90GbmoAGAeY"
  resp = requests.post(url, headers=headers, data=data)
  respj = resp.json()
  token = respj['access_token']
  url = "https://test.api.amadeus.com/v2/shopping/flight-offers?originLocationCode={}&destinationLocationCode={}&departureDate={}&returnDate={}&adults=1&infants=0&travelClass=ECONOMY&nonStop=true&currencyCode=USD&max=1".format(
    origin_city, dest_city, dep_date, return_date)
  # replace the token with one fresh from prev script
  headers = CaseInsensitiveDict()
  headers["Authorization"] = "Bearer {}".format(token)
  # headers["Authorization"] = "Bearer dikJzPvVabM37k7UwEELEksjC1Mn"
  resp = requests.get(url, headers=headers)
  respj = resp.json()
  data1 = respj['data'][0]
  price_total = data1['price']['total']
  return price_total

#### Updating The Relevant Request In MongoDB As Not Active ####
def update_mongo_not_active(request_id):
  myquery = {"_id": request_id}
  newvalues = {"$set": {"is_active": 0}}
  mycol.update_one(myquery, newvalues)

#### Updating The New Request In MongoDB With The New Price ####
def insert_mongo_new_price(origin_city , dest_city , dep_date , return_date , user_id , chat_id , user_last_name ,user_user_name
                           ,user_first_name, carrier , request_type , duration , new_price , price_currency , cabin,current_timestamp):
  mydict = {"origin_city":origin_city
    , "dest_city":dest_city
    , "dep_date":dep_date
    , "return_date":return_date
    , "user_id":user_id
    , "chat_id":chat_id
    , "user_last_name":user_last_name
    , "user_user_name":user_user_name
    , "user_first_name":user_first_name
    , "carrier":carrier
    , "request_type":request_type
    , "duration":duration
    , "price_total":new_price
    , "price_currency":price_currency
    , "cabin":cabin
    ,"current_ts":current_timestamp
    ,"is_active":1}
  x = mycol.insert_one(mydict)


#### Quering MongoDB Only With The Active Requests ####
myquery = { "is_active": 1 }
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Amadeus_DB"]
mycol = mydb["Flights_Requests"]

#### Pulling The Parameters Of The Active Requests In Order To Send To Amadeus API ####
for x in mycol.find(myquery):
  request_id = x["_id"]
  origin_city = x["origin_city"]
  dest_city = x["dest_city"]
  dep_date = x["dep_date"]
  return_date = x["return_date"]
  original_price_total = x["price_total"]
  user_id = x["user_id"]
  user_user_name = x["user_user_name"]
  user_first_name = x["user_first_name"]
  user_last_name = x["user_last_name"]
  chat_id = x["chat_id"]
  carrier = x["carrier"]
  request_type = x["request_type"]
  duration = x["duration"]
  price_currency = x["price_currency"]
  cabin = x["cabin"]

  #### Getting The New Price Back From API ####
  new_price = amadeus_check_price(origin_city, dest_city, dep_date, return_date)

  #### Checking If The New Price Is Lower Than The Old One ####
  if new_price<original_price_total:

    #### Sending An Update To The User Via Telegram ####
    send_message_to_user(chat_id,user_first_name,new_price,origin_city, dest_city, dep_date, return_date,original_price_total)

    #### Sending The Request In Order To Turn To Not Active ####
    update_mongo_not_active(request_id)

    #### Inserting MongoDB With New Row ####
    insert_mongo_new_price(origin_city , dest_city , dep_date , return_date , user_id , chat_id , user_last_name ,user_user_name
                           ,user_first_name, carrier , request_type , duration , new_price , price_currency , cabin,current_timestamp)