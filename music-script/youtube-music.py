import requests
import datetime
import json
import random
import time
from googleapiclient.discovery import build
import schedule
import os

def fetch_youtube_data():
    stream_info_list = []


    api_key = "AIzaSyCy-Nalr01SeJhCuc1Z7YbVu0vmGTxYIK8"
    playlist_id = "PL4fGSI1pDJn5BPviUFX4a3IMnAgyknC68"

    youtube = build('youtube', 'v3', developerKey=api_key)

    playlist_response = youtube.playlists().list(
        part='snippet',
        id=playlist_id
    ).execute()

    playlist_items_response = youtube.playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=100
    ).execute()

    for index, item in enumerate(playlist_items_response['items']):
        video_id = item['snippet']['resourceId']['videoId']
        video_title = item['snippet']['title']

        video_response = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=video_id
        ).execute()

        view_count = video_response['items'][0]['statistics']['viewCount']
        artist_name = video_response['items'][0]['snippet']['title'].split(' - ')[0]
        like_count = video_response['items'][0]['statistics']['likeCount']
        comment_count = video_response['items'][0]['statistics']['commentCount']

        current_time = int(time.time() * 1000)
        id_value = random.randint(current_time, current_time + 9999)

        stream_info = {
            'id': id_value,
            'dateTime': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'title': video_title,
            'artist': artist_name,
            'likes': like_count,
            'comments': comment_count,
            'streams': view_count,
            'position': index + 1,
        }

        stream_info_list.append(stream_info)
        print(f"Added: {video_title} to the list")

    for stream_info in stream_info_list:
        print(f"{stream_info['title']} - Likes: {stream_info['likes']} - Comments: {stream_info['comments']} - Number of streams: {stream_info['streams']} - Position: {stream_info['position']}")

        data = {
            'id': stream_info['id'],
            'dateTime': stream_info['dateTime'],
            'title': stream_info['title'],
            'artist': stream_info['artist'],
            'likes': stream_info['likes'],
            'comments': stream_info['comments'],
            'streams': stream_info['streams'],
            'position': stream_info['position'],
        }

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        url = 'http://fluent-bit:9090'
        response = requests.post(url, data=json.dumps(data), headers=headers)
        print(f"Sent to Fluent with response: {response}")


def job():
    print("Fetching YouTube data...")
    fetch_youtube_data()
    print("Job completed.")

# Esegui la funzione `fetch_youtube_data` immediatamente
fetch_youtube_data()

# Esegui la funzione `fetch_youtube_data` ogni 5 minuti
schedule.every(55).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
