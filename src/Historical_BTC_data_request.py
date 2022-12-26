import sqlite3
import requests
import sys

con = sqlite3.connect("BTCHist.db")
cur = con.cursor()
#cur.execute("DROP TABLE btc") #Every iteration of this code I am starting with a fresh DB for testing purposes for now
cur.execute("CREATE TABLE IF NOT EXISTS btc (seconds INTEGER, open DECIMAL, high DECIMAL, low DECIMAL, close DECIMAL, vwap DECIMAL, volume DECIMAL, count INTEGER)")

resp = requests.get('https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1&since=1622195200') #It appears that no matter what since value I send, it always pulls most recent data points from API.
data = resp.json()
print(resp.json())
for value in data['result']['XXBTZUSD']:
    cur.execute("INSERT INTO btc (seconds, open, high, low, close, vwap, volume, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", value)

con.commit() #I still commit each time because I am checking my work in the actual DB

cur.execute("SELECT seconds FROM btc ORDER BY seconds DESC LIMIT 1")
lastSecondTup = cur.fetchone()
lastSecond = lastSecondTup[0]
print(lastSecond)
newSecond = 0;
sys.exit() 
while lastSecond != newSecond:
    for value in data['result']['XXBTZUSD']:#This inner loop will collect 720 data point.
        cur.execute("INSERT INTO btc (seconds, open, high, low, close, vwap, volume, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", value)
    
    resp = requests.get('https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1&since=' + str(lastSecond))
    data = resp.json()
    lastSecond = newSecond
    newSecond = data['last']



con.commit()

