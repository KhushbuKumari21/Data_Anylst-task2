import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from pymongo import MongoClient

# Connect to MongoDB
try:
    client = MongoClient('localhost', 27017)
    db = client['stock_market_data']
    collection = db['ICICI_data']
    print("Connected to MongoDB successfully.")
except Exception as e:
    print(f"Failed to connect to MongoDB. Error: {e}")

# Function to fetch and store real-time data
def fetch_and_store_data():
    try:
        now = datetime.now()
        print(f"Current time: {now}")

        if now.time() >= datetime.strptime('11:15:00', '%H:%M:%S').time() and now.time() <= datetime.strptime('14:15:00', '%H:%M:%S').time():
            print("Current time falls within the specified range.")
            ticker = "ICICIBANK.NS"
            icici = yf.Ticker(ticker)
            if icici:
                print("Connected to Yahoo Finance API successfully.")
                data = icici.history(period="1d", interval="15m")
                latest_data = data.iloc[-1]

                record = {
                    'Datetime': latest_data.name,
                    'Open': latest_data['Open'],
                    'High': latest_data['High'],
                    'Low': latest_data['Low'],
                    'Close': latest_data['Close'],
                    'Volume': latest_data['Volume']
                }
                collection.insert_one(record)
                print(f"Data stored for {latest_data.name}")
            else:
                print("Failed to connect to Yahoo Finance API.")
        else:
            print("Current time does not fall within the specified range.")

    except Exception as e:
        print("An error occurred: ", e)

# Create an APScheduler and schedule the job
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_and_store_data, 'interval', minutes=15)
scheduler.start()

# Schedule the script to run for a week
end_time = datetime.now() + timedelta(days=7)
try:
    while datetime.now() < end_time:
        pass
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()