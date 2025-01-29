import re
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
import streamlit as st
import time
from pymongo.errors import ServerSelectionTimeoutError

# Ensure MongoDB URI is available
if "mongo" not in st.secrets or "MONGO_URI" not in st.secrets["mongo"]:
    st.error("MongoDB URI is missing in Streamlit secrets! Check your `secrets.toml`.")
    st.stop()

MONGO_URI = st.secrets["mongo"]["MONGO_URI"]

# Function to connect to MongoDB with retries
def connect_to_mongo(retries=3, delay=5):
    for attempt in range(retries):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=20000)
            client.admin.command("ping")  # Test connection
            return client
        except ServerSelectionTimeoutError as e:
            st.warning(f"MongoDB Connection Failed (Attempt {attempt+1}): {e}")
            time.sleep(delay)
    
    st.error("MongoDB Connection Failed after multiple attempts.")
    st.stop()
    return None

client = connect_to_mongo()
if client:
    db = client["chat_data"]

def preprocess(data):
    pattern = r'\d{1,2}/\d{1,2}/\d{2,4},\s\d{1,2}:\d{1,2}\s-\s'

    messages = re.split(pattern, data)[1:]
    dates = re.findall(pattern, data)
    dates = [date.strip() for date in dates]

    df = pd.DataFrame({'user_message': messages, 'message_date': dates})

    # Generate a safe collection name
    filename = 'chat_' + datetime.now().strftime('%Y%m%d_%H%M%S')
    collection = db[filename]

    # Try parsing dates in multiple formats
    try:
        df['message_date'] = pd.to_datetime(df['message_date'], format='%d/%m/%y, %H:%M -', dayfirst=True)
    except ValueError:
        df['message_date'] = pd.to_datetime(df['message_date'], format='%d/%m/%Y, %H:%M -', dayfirst=True)

    df.rename(columns={'message_date': 'date'}, inplace=True)

    users = []
    messages = []
    for message in df['user_message']:
        entry = re.split(r'([\w\W]+?):\s', message)
        if entry[1:]:  # user name
            users.append(entry[1])
            messages.append(" ".join(entry[2:]))
        else:
            users.append('group_notification')
            messages.append(entry[0])

    df['user'] = users
    df['message'] = messages
    df.drop(columns=['user_message'], inplace=True)

    df['only_date'] = df['date'].dt.date
    df['year'] = df['date'].dt.year
    df['month_num'] = df['date'].dt.month
    df['month'] = df['date'].dt.month_name()
    df['day'] = df['date'].dt.day
    df['day_name'] = df['date'].dt.day_name()
    df['hour'] = df['date'].dt.hour
    df['minute'] = df['date'].dt.minute

    # Add a 'period' column
    df['period'] = df['hour'].apply(lambda h: f"{h}-00" if h == 23 else "00-1" if h == 0 else f"{h}-{h+1}")

    # Only insert non-empty data
    data = df.to_dict(orient="records")
    if data:
        collection.insert_many(data)
    else:
        st.warning("No messages found to insert into MongoDB.")

    return df
