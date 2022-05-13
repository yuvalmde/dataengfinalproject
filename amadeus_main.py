###TELEGRAM#### -
### BOT NAME - @TickeTBot ###
respj3 = ''
TOKEN = "5151016014:AAFIwk5NgGNeYHdJNVBmj_pQtQqHcjvtlok"
import datetime
from datetime import date
import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
from kafka import KafkaProducer
import json
import telebot
import boto3
bot = telebot.TeleBot(TOKEN)

####################### AMADEUS Requests #################
def amadeus(request_type, origin_city, dep_date, dest_city='NYC', return_date='2999-01-01', duration='1',
            maxPrice='1000'):
    if request_type == 'city':
        url = "https://test.api.amadeus.com/v2/shopping/flight-offers?originLocationCode={}&destinationLocationCode={}&departureDate={}&returnDate={}&adults=1&infants=0&travelClass=ECONOMY&nonStop=true&currencyCode=USD&max=1".format(
            origin_city, dest_city, dep_date, return_date)
        resp = requests.get(url, headers=headers)
        return resp
    if request_type == 'inspire':
        url = "https://test.api.amadeus.com/v1/shopping/flight-destinations?origin={}&departureDate={}&oneWay=false&duration={}&nonStop=true&maxPrice={}&viewBy=COUNTRY".format(
            origin_city, dep_date, duration, maxPrice)
        resp = requests.get(url, headers=headers)
        return resp

####################### Generating Temp Amadeus Token ###################

url = "https://test.api.amadeus.com/v1/security/oauth2/token"
headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/x-www-form-urlencoded"
data = "grant_type=client_credentials&client_id=eMssgdZ69DLAO3jUZLJvircW9yoJrq9s&client_secret=idNOH90GbmoAGAeY"
resp = requests.post(url, headers=headers, data=data)
respj = resp.json()
token = respj['access_token']
print("Amadeus Token: ",token)
headers = CaseInsensitiveDict()
headers["Authorization"] = "Bearer {}".format(token)


###########################################################################


##################### Data Frame Enrichment #############
def enrich_df(df, dict):
    df1 = df.copy()
    for k, v in dict.items():
        df1[k] = v
    return df1

###################### Start ############################
def start(message):
    request = message.text
    if request == "/start" or request == "start":
        return True
    else:
        return False

@bot.message_handler(func=start)
def start(message):
  resp = '''Hi! I'm TickeBot
I will help you to find the cheapest ticket and inspire destinations fast and simple!
I can search for the tickets in two ways:
Type city and then Original City, Destination City, Department Date and Return Date
and I will find the cheapest ticket for you
Example: city TLV NYC 2022-07-01 2022-07-31
Type inspire and then Original City, Duration and Max Price
and I will find you 3 possible destinations
Example: inspire TLV 2022-07-01 10 400

Bonus: I will re-search your City type requests and update you once a better price will be found!
Isn't this great?'''
  bot.send_message(message.chat.id, resp)


#########################################################


##################### Search City #######################
def city_request(message):
    request = message.text.split()
    resp = "Please Type 'city' Or 'inspire' and other search parameters"
    if request[0].lower() in "inspire":
        return False
    elif len(request) < 5 or request[0].lower() not in "city":
        bot.send_message(message.chat.id, resp)
        return False
    else:
        return True

@bot.message_handler(func=city_request)
def send_price_city(message):
    origin_city = message.text.split()[1]
    dest_city = message.text.split()[2]
    dep_date = message.text.split()[3]
    return_date = message.text.split()[4]
    user_id = message.from_user.id
    user_user_name = message.from_user.username
    user_first_name = message.from_user.first_name
    user_last_name = message.from_user.last_name
    chat_id = message.chat.id
    resp2 = amadeus('city', origin_city=origin_city, dep_date=dep_date, dest_city=dest_city, return_date=return_date)
    respj2 = resp2.json()

    df = pd.json_normalize(respj2)
    #### Error or empty response handling ####
    if df.filter(items=['errors']).shape[1]==1 or (len(respj2['data'])) ==0:
      resp = 'I did not find anything for your request, try one more time'
      bot.send_message(message.chat.id, resp)
      return

    #### Parsing dataframe ####
    elif df.filter(items=['data']).shape[1]==1:
      data1 = respj2['data'][0]
      dictionaries = respj2['dictionaries']
      price_total = data1['price']['total']
      price_currency = data1['price']['currency']
      cabin = data1['travelerPricings'][0]['fareDetailsBySegment'][0]['cabin']
      duration = data1['itineraries'][0]['duration'][2:]

      carrier = dictionaries['carriers']
      for key, value in carrier.items():
          carrier_val = value
      resp = "Total Price: ${}\nCarrier: {}".format(price_total, carrier_val)
      df = pd.json_normalize(respj2)

      bot.send_message(message.chat.id, resp)
        #### Dataframe Enrichment ####
      df_enrich_dict = {'origin_city': origin_city, 'dest_city': dest_city, 'dep_date': dep_date,
                        'return_date': return_date ,'chat_id':chat_id,'user_id': user_id, 'user_user_name': user_user_name,
                        'user_first_name': user_first_name, 'user_last_name': user_last_name, 'carrier': carrier_val,
                        'request_type': 'City', 'duration':duration, 'price_total':price_total,'price_currency':price_currency,'cabin':cabin}
      df_enr = enrich_df(df, df_enrich_dict)


    send_to_kafka(df_enr)
    send_to_s3(df_enr)

##################### Second API - Search Inspire ####################
def inspire_request(message):
    request = message.text.split()
    if len(request) < 5 or request[0].lower() not in "inspire":
        return False
    else:
        return True


@bot.message_handler(func=inspire_request)
def send_inspire(message):
    i = 0
    origin_city = message.text.split()[1]
    dep_date = message.text.split()[2]
    duration = message.text.split()[3]
    maxPrice = message.text.split()[4]
    resp2 = amadeus('inspire', origin_city=origin_city, dep_date=dep_date, duration=duration, maxPrice=maxPrice)
    respj = resp2.json()
    df = pd.json_normalize(respj)
    #### Error or empty response handling ####
    if df.filter(items=['errors']).shape[1]==1 or (len(respj['data'])) ==0:
     resp = 'I did not find anything for your request, try one more time'
     bot.send_message(message.chat.id, resp)
     return
    #### Datafreame Parsing ####
    elif df.filter(items=['data']).shape[1]==1:
      #### We send 3 random destinations out of all results ####
      print("respj",len(respj))
      for i in range(0,len(respj)):
        dest = respj['data'][i]['destination']
        returnDate = respj['data'][i]['returnDate']
        price = respj['data'][i]['price']['total']
        resp = "{} Destination: {}\nReturn Date: {} \nPrice: ${}".format(i+1,dest, returnDate,price)
        bot.send_message(message.chat.id, resp)
        i+=1
    #### Pulling Data From Telegram Bot ####
    user_id = message.from_user.id
    user_username = message.from_user.username
    user_name = message.from_user.first_name
    user_lastname = message.from_user.last_name

######SEND TO KAFKA#########
def send_to_kafka(df):
    # Topics/Brokers
    topic1 = 'amadeus'
    brokers = ['Cnt7-naya-cdh63:9092']
    producer = KafkaProducer(bootstrap_servers=brokers)
    #### Getting the data ready for kafka ####
    row = df.to_dict(orient='records')[0]
    row_json_str = json.dumps(row)
    producer.send(topic1, value=row_json_str.encode('utf-8'))
    producer.flush()

def send_to_s3(df):
    #### Creating today's date and current timestamp in order to dynamically generate directory and file name in S3 bucket ####
    today = date.today()
    current_date = today.strftime("%d_%m_%Y")

    def format_time():
        t = datetime.datetime.now()
        s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
        return s[:-3]

    new_ts = format_time()
    current_timestamp = new_ts.split()[1].replace(":", "_").split(".")[0]

    #### Converting Dataframe to JSON ####
    json_df = pd.DataFrame(data=df)
    json_df = json_df.to_json()
    json_object = json.loads(json_df)

    #### Creating S3 session using boto3 ####
    s3 = boto3.client("s3", \
                      region_name='us-east-1', \
                      aws_access_key_id='AKIAW7Z4UEFXESZ5WLUK', \
                      aws_secret_access_key='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')

    #### Uploading JSON File To S3 Bucket By Date Partition ####
    s3.put_object(Bucket='my-amadeus',  Body=(bytes(json.dumps(json_object).encode('UTF-8'))), Key=f'request/{current_date}/{current_timestamp}.json')

bot.polling()


