import re
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
import streamlit as st

MONGO_URI = st.secrets["mongo"]["MONGO_URI"]

def preprocess(data):
    pattern = '\d{1,2}/\d{1,2}/\d{2,4},\s\d{1,2}:\d{1,2}\s-\s'

    messages = re.split(pattern, data)[1:]
    dates = re.findall(pattern, data)

    # Strip the trailing whitespace
    dates = [date.strip() for date in dates]

    df = pd.DataFrame({'user_message': messages, 'message_date': dates})
    filename='chat' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client["chat_data"]
    collection = db[filename]
    data = df.to_dict(orient="records")
    collection.insert_many(data)
    df['message_date'] = pd.to_datetime(df['message_date'], format='%d/%m/%y, %H:%M -', dayfirst=True)

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

    # Optional: Add a 'period' column if needed
    period = []
    for hour in df[['day_name', 'hour']]['hour']:
        if hour == 23:
            period.append(f"{hour}-00")
        elif hour == 0:
            period.append("00-1")
        else:
            period.append(f"{hour}-{hour + 1}")
    df['period'] = period

    return df
