# Import necessary libraries
import streamlit as st
import mysql.connector
from pymongo import MongoClient
from googleapiclient.discovery import build

# Set up MySQL connection
mysql_host = 'localhost'
mysql_user = 'your_mysql_username'
mysql_password = 'your_mysql_password'
mysql_database = 'youtube_data'

mysql_conn = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)

mysql_cursor = mysql_conn.cursor()

# Set up MongoDB connection
mongo_host = 'localhost'
mongo_port = 27017
mongo_database = 'youtube_data'
mongo_collection = 'videos'

mongo_client = MongoClient(mongo_host, mongo_port)
mongo_db = mongo_client[mongo_database]
mongo_coll = mongo_db[mongo_collection]

# Set up YouTube API credentials
api_key = 'your_youtube_api_key'
youtube = build('youtube', 'v3', developerKey=api_key)


# Streamlit app
def main():
    st.title('YouTube Data Harvesting and Warehousing')

    # Harvest YouTube data
    st.header('Data Harvesting')
    channel_id = st.text_input('Enter YouTube Channel ID:')
    if st.button('Harvest Data'):
        harvest_data(channel_id)

    # Display data from SQL and MongoDB
    st.header('Data Warehousing')
    display_data()


def harvest_data(channel_id):
    # Clear existing data from SQL and MongoDB
    clear_data()

    # Harvest videos from YouTube channel
    videos = youtube_search(channel_id)
    for video in videos:
        # Store video data in SQL
        store_in_sql(video)

        # Store video data in MongoDB
        store_in_mongodb(video)

    st.success('Data harvested successfully!')


def clear_data():
    # Clear data from SQL
    sql_query = 'DELETE FROM videos;'
    mysql_cursor.execute(sql_query)
    mysql_conn.commit()

    # Clear data from MongoDB
    mongo_coll.delete_many({})


def youtube_search(channel_id):
    videos = []

    # Retrieve videos using YouTube API
    request = youtube.search().list(
        part='snippet',
        channelId=channel_id,
        maxResults=10
    )
    response = request.execute()

    for item in response['items']:
        video_data = {
            'title': item['snippet']['title'],
            'description': item['snippet']['description'],
            'published_at': item['snippet']['publishedAt']
            # Add more data fields as needed
        }
        videos.append(video_data)

    return videos


def store_in_sql(video):
    sql_query = '''
        INSERT INTO videos (title, description, published_at)
        VALUES (%s, %s, %s)
    '''
    values = (video['title'], video['description'], video['published_at'])
    mysql_cursor.execute(sql_query, values)
    mysql_conn.commit()


def store_in_mongodb(video):
    mongo_coll.insert_one(video)


def display_data():
    # Display data from SQL
    st.subheader('Data from SQL')
    sql_query = 'SELECT * FROM videos LIMIT 10;'
    mysql_cursor.execute(sql_query)
    result = mysql_cursor.fetchall()
    st.table(result)

    # Display data from MongoDB
    st.subheader('Data from MongoDB')
    result = mongo_coll.find().limit(10)
    st.table(result)


# Run the Streamlit app
if __name__ == '__main__':
    main()
