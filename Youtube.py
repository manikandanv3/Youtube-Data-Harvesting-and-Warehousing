import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import mysql.connector
import re
from datetime import timedelta, datetime

# Your API Key here
API_KEY = 'AIzaSyCYF0spPJdkeUHDoHgC5fldppDZGibE2HQ'

# Set up YouTube API client
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Define the 10 YouTube channel IDs


channel_ids = [
    'UCCezIgC97PvUuR4_gbFUs5g', 'UCfzlCWGWYyIQ0aLC5w48gBQ', 'UC7cs8q-gJRlGwj4A8OmCmXg',
    'UCOqXBtP8zUeb3jqeJ7gS-EQ', 'UCWeOtlakw8g01MrR8U4yYtg', 'UCFp1vaKzpfvoGai0vE5VJ0w',
    'UCNU_lfiiWBdtULKOw6X0Dig', 'UCcIXc5mJsHVYTZR1maL5l9w', 'UCtYLUTtgS3k1Fg4y5tAhLbw',
    'UCVLbzhxVTiTLiVKeGV7WEBg'
]

def Get_Channel_details(youtube, channel_ids):
    channel_list = []
    request = youtube.channels().list(part='snippet,contentDetails,statistics', id=','.join(channel_ids))
    response = request.execute()
    for item in response['items']:
        data = {
            "channel_name": item['snippet']['title'],
            "channel_id": item['id'],
            "subcription_count": item['statistics']['subscriberCount'],
            "channel_views": item['statistics']['viewCount'],
            "channel_description": item['snippet']['description'],
            "playlist_id": item['contentDetails']['relatedPlaylists']['uploads'],
            "video_count": item['statistics']['videoCount']
        }
        channel_list.append(data)
    return channel_list

def play_list_(channel_id):
    all_data = []
    request = youtube.playlists().list(part="snippet,id", channelId=channel_id, maxResults=10)
    response = request.execute()
    for item in response["items"]:
        data = {
            "playlist_id": item["id"],
            "channel_id": item["snippet"]["channelId"],
            "playlist_name": item["snippet"]["title"]
        }
        all_data.append(data)
    return all_data

def playlist_id(channel_data):
    return [item["playlist_id"] for item in channel_data]

def Get_video_details(youtube, playlist_ids):
    video_id = []
    for pid in playlist_ids:
        request = youtube.playlistItems().list(part="snippet,contentDetails", playlistId=pid, maxResults=50)
        response = request.execute()
        for item in response["items"]:
            video_id.append(item["contentDetails"]["videoId"])
    return video_id

def Get_video_data(youtube, video_ids):
    all_data = []
    for vid in video_ids:
        request = youtube.videos().list(part="snippet,statistics,contentDetails", id=vid)
        response = request.execute()
        for item in response["items"]:
            duration = item['contentDetails']['duration']
            matches = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration)
            hours = int(matches.group(1)[:-1]) if matches.group(1) else 0
            minutes = int(matches.group(2)[:-1]) if matches.group(2) else 0
            seconds = int(matches.group(3)[:-1]) if matches.group(3) else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
            duration_obj = timedelta(seconds=total_seconds)
            new_duration = str(duration_obj)

            data = {
                "video_id": item["id"],
                "video_name": item["snippet"]["title"],
                "video_description": item["snippet"]["description"],
                "published_At": item["snippet"]["publishedAt"],
                "view_count": item["statistics"].get("viewCount", 0),
                "like_count": item["statistics"].get("likeCount", 0),
                "favorite_count": item["statistics"].get("favoriteCount", 0),
                "duration": new_duration,
                "thumbnails": item["snippet"]["thumbnails"]["default"]["url"],
                "comment_count": item["statistics"].get("commentCount", 0),
                "caption_status": item["contentDetails"]["caption"]
            }
            all_data.append(data)
    return all_data

def comment_data(video_ids):
    all_data = []
    for vid in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="id,snippet,replies",
                videoId=vid,
                maxResults=10
            )
            response = request.execute()
            for item in response["items"]:
                data = {
                    "comment_id": item["id"],
                    "video_id": vid,
                    "comment_text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "comment_author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "comment_publishedAt": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                }
                all_data.append(data)
        except:
            pass  # Skip videos with disabled comments
    return all_data

# Start Streamlit app
st.title("YouTube Data Harvesting and Warehousing")

selected_channel_id = st.sidebar.selectbox("Select a channel ID", channel_ids)

if selected_channel_id:
    channel_data = Get_Channel_details(youtube, [selected_channel_id])
    playlist_data = play_list_(selected_channel_id)
    playlist_ids = playlist_id(channel_data)
    video_ids = Get_video_details(youtube, playlist_ids)
    video_data = Get_video_data(youtube, video_ids)
    comment_data_list = comment_data(video_ids)

    # Insert data into MySQL database
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password='root',
            auth_plugin="mysql_native_password"
        )

        mycursor = mydb.cursor()

        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_data")
        mycursor.execute("USE youtube_data")

        # Create tables
        mycursor.execute("""
        CREATE TABLE IF NOT EXISTS Channel_details (
            channel_id VARCHAR(250) PRIMARY KEY, 
            channel_name VARCHAR(250),
            subcription_count INT, 
            channel_views BIGINT,
            channel_description TEXT, 
            playlist_id VARCHAR(250),
            video_count INT
        )""")

        mycursor.execute("""
        CREATE TABLE IF NOT EXISTS playlist_details (
            playlist_id VARCHAR(250) PRIMARY KEY, 
            channel_id VARCHAR(250),
            playlist_name VARCHAR(250)
        )""")

        mycursor.execute("""
        CREATE TABLE IF NOT EXISTS video_details (
            video_id VARCHAR(250) PRIMARY KEY, 
            playlist_ids VARCHAR(255),
            video_name VARCHAR(250), 
            published_At DATETIME,
            video_description TEXT, 
            view_count INT,
            like_count INT, 
            favorite_count INT,
            thumbnails VARCHAR(250),
            comment_count INT,
            caption_status VARCHAR(250), 
            duration TIME
        )""")

        mycursor.execute("""
        CREATE TABLE IF NOT EXISTS comment_details (
            comment_id VARCHAR(250) PRIMARY KEY, 
            video_id VARCHAR(250),
            comment_text TEXT, 
            comment_author VARCHAR(250),
            comment_publishedAt DATETIME
        )""")

        # Insert data
        for row in channel_data:
            query = """
            INSERT INTO Channel_details (
                channel_id, channel_name, subcription_count,
                channel_views, channel_description,
                playlist_id, video_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                channel_name = VALUES(channel_name),
                subcription_count = VALUES(subcription_count),
                channel_views = VALUES(channel_views),
                channel_description = VALUES(channel_description),
                playlist_id = VALUES(playlist_id),
                video_count = VALUES(video_count)
            """
            mycursor.execute(query, (
                row['channel_id'], row['channel_name'], row['subcription_count'],
                row['channel_views'], row['channel_description'],
                row['playlist_id'], row['video_count']
            ))

        for row in playlist_data:
            query = """
            INSERT INTO playlist_details (
                playlist_id, channel_id, playlist_name
            ) VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                channel_id = VALUES(channel_id),
                playlist_name = VALUES(playlist_name)
            """
            mycursor.execute(query, (
                row['playlist_id'], row['channel_id'], row['playlist_name']
            ))

        for row in video_data:
            published_at = datetime.strptime(row['published_At'], '%Y-%m-%dT%H:%M:%SZ')
            published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')
            query = """
            INSERT INTO video_details (
                video_id, playlist_ids, video_name, published_At,
                video_description, view_count, like_count,
                favorite_count, thumbnails, comment_count,
                caption_status, duration
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                playlist_ids = VALUES(playlist_ids),
                video_name = VALUES(video_name),
                published_At = VALUES(published_At),
                video_description = VALUES(video_description),
                view_count = VALUES(view_count),
                like_count = VALUES(like_count),
                favorite_count = VALUES(favorite_count),
                thumbnails = VALUES(thumbnails),
                comment_count = VALUES(comment_count),
                caption_status = VALUES(caption_status),
                duration = VALUES(duration)
            """
            mycursor.execute(query, (
                row['video_id'], ','.join(playlist_ids), row['video_name'],
                published_at, row['video_description'], row['view_count'],
                row['like_count'], row['favorite_count'], row['thumbnails'],
                row['comment_count'], row['caption_status'], row['duration']
            ))

        for row in comment_data_list:
            comment_publishedAt = datetime.strptime(row['comment_publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            comment_publishedAt = comment_publishedAt.strftime('%Y-%m-%d %H:%M:%S')
            query = """
            INSERT INTO comment_details (
                comment_id, video_id, comment_text,
                comment_author, comment_publishedAt
            ) VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                video_id = VALUES(video_id),
                comment_text = VALUES(comment_text),
                comment_author = VALUES(comment_author),
                comment_publishedAt = VALUES(comment_publishedAt)
            """
            mycursor.execute(query, (
                row['comment_id'], row['video_id'], row['comment_text'],
                row['comment_author'], comment_publishedAt
            ))

        mydb.commit()
        st.sidebar.success("Data successfully inserted into the database!")
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
    finally:
        if 'mycursor' in locals():
            mycursor.close()
        if 'mydb' in locals():
            mydb.close()

    # Create tabs to display data
    tab_selection = st.sidebar.radio("Go to:", ["Data", "Analysis"])

    
    if tab_selection == "Data":
        st.subheader("Channel Data:")
        st.write(pd.DataFrame(channel_data))
        st.subheader("Video Data:")
        st.write(pd.DataFrame(video_data))
        st.subheader("Comment Data:")
        st.write(pd.DataFrame(comment_data_list))

    elif tab_selection == "Analysis":
        st.subheader("Analysis")
        query_options = [
                "What are the names of all the videos and their corresponding channels?",
                "Which channels have the most number of videos, and how many videos do they have?",
                "What are the top 10 most viewed videos and their respective channels?",
                "How many comments were made on each video, and what are their corresponding video names?",
                "Which videos have the highest number of likes, and what are their corresponding channel names?",
                "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                "What is the total number of views for each channel, and what are their corresponding channel names?",
                "What are the names of all the channels that have published videos in the year 2023?",
                "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                "Which videos have the highest number of comments, and what are their corresponding channel names?"
        ]

        selected_query = st.selectbox("Select a query", query_options)

        if selected_query:
            mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password='root',
            database="youtube_data",
            auth_plugin="mysql_native_password"
        )
        mycursor = mydb.cursor()

        if selected_query == query_options[0]:
            query = "SELECT v.video_name, c.channel_name FROM video_details v JOIN Channel_details c ON v.playlist_ids LIKE CONCAT('%', c.playlist_id, '%')"
        elif selected_query == query_options[1]:
            query = "SELECT c.channel_name, COUNT(v.video_id) as num_videos FROM video_details v JOIN Channel_details c ON v.playlist_ids LIKE CONCAT('%', c.playlist_id, '%') GROUP BY c.channel_name ORDER BY num_videos DESC"
        elif selected_query == query_options[2]:
            query = "SELECT v.video_name, c.channel_name, v.view_count FROM video_details v JOIN Channel_details c ON v.playlist_ids LIKE CONCAT('%', c.playlist_id, '%') ORDER BY v.view_count DESC LIMIT 10"
        elif selected_query == query_options[3]:
            query = "SELECT v.video_name, COUNT(cm.comment_id) as num_comments FROM video_details v LEFT JOIN comment_details cm ON v.video_id = cm.video_id GROUP BY v.video_name"
        elif selected_query == query_options[4]:
            query = "SELECT v.video_name, c.channel_name, v.like_count FROM video_details v JOIN Channel_details c ON v.playlist_ids LIKE CONCAT('%', c.playlist_id, '%') ORDER BY v.like_count DESC"
        elif selected_query == query_options[5]:
            query = "SELECT v.video_name, v.like_count, v.favorite_count FROM video_details v"
        elif selected_query == query_options[6]:
            query = "SELECT c.channel_name, SUM(v.view_count) as total_views FROM video_details v JOIN Channel_details c ON v.playlist_ids LIKE CONCAT('%', c.playlist_id, '%') GROUP BY c.channel_name"
        elif selected_query == query_options[7]:
            query = "SELECT DISTINCT c.channel_name FROM Channel_details c JOIN video_details v ON v.playlist_ids LIKE CONCAT('%', c.playlist_id, '%') WHERE YEAR(v.published_At) = 2023"
        elif selected_query == query_options[8]:
            query = "SELECT c.channel_name, AVG(TIME_TO_SEC(v.duration)) as avg_duration FROM video_details v JOIN Channel_details c ON v.playlist_ids LIKE CONCAT('%', c.playlist_id, '%') GROUP BY c.channel_name"
        elif selected_query == query_options[9]:
            query = "SELECT v.video_name, c.channel_name, COUNT(cm.comment_id) as num_comments FROM video_details v JOIN Channel_details c ON v.playlist_ids LIKE CONCAT('%', c.playlist_id, '%') LEFT JOIN comment_details cm ON v.video_id = cm.video_id GROUP BY v.video_name, c.channel_name ORDER BY num_comments DESC"

        mycursor.execute(query)
        result = mycursor.fetchall()

        # Display the result of the query
        st.write("Result:")
        for row in result:
            st.write(row)

        mycursor.close()
        mydb.close()