import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
import mysql.connector
import isodate
from datetime import datetime
import pandas as pd
import mysql.connector  
from isodate import parse_duration 
from dateutil import parser


api_key = 'AIzaSyAxBwKM7x5AGG6_n-GGdc5SOTEcGADDUzI'
youtube = build('youtube', 'v3', developerKey=api_key)

def channel(channel_id):

    api_key = 'AIzaSyAxBwKM7x5AGG6_n-GGdc5SOTEcGADDUzI'

    youtube = build('youtube', 'v3', developerKey=api_key)

    all_data = []
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=(channel_id)
)
    response = request.execute()
    
    
    for i in response['items']:
        data = { 'channel_name':i['snippet']['title'],
            'subscription':i['statistics']['subscriberCount'],
            'views':i['statistics']['viewCount'],
            'totel_videos':i['statistics']['videoCount'],
            'playlist_id':i['contentDetails']['relatedPlaylists']['uploads'],
            'published_date':i['snippet']['publishedAt']
           }
    all_data.append(data)    
    
    return all_data



def get_video_ids(youtube, playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=5
    )
    response = request.execute()
    for item in response['items']:
        video_id = item['contentDetails']['videoId']
        video_ids.append(video_id)
    return video_ids


def get_video_details(video_id):
    all_video_details = []
    request = youtube.videos().list(part="snippet,contentDetails,statistics",id=video_id)
    response = request.execute()
    item = response['items'][0]
    stats = {
            'video_id' : video_id,
            'title': item['snippet']['title'],
            'view_count': item['statistics']['viewCount'],
            'like_count': item['statistics']['likeCount'],
            'comment_count' : item['statistics']['commentCount'],
            'duration':  item['contentDetails']['duration'],
            'definition': item['contentDetails']['definition'],
             }
    return stats


def get_video_comments(youtube, video_id):
    comments = []
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=5
    )
    response = request.execute()
    
    while 'items' in response:
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comment_text = comment['textDisplay']
            author_display_name = comment['authorDisplayName']
            publish_date = comment['publishedAt']
            like_count = comment['likeCount']
            
            comments.append({
                'text': comment_text,
                'author': author_display_name,
                'date': publish_date,
                'likes': like_count
            })

        if 'nextPageToken' in response:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=5,
                pageToken=response['nextPageToken']
            )
            response = request.execute()
        else:
            break

    return comments


def execute_query(query):
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345",
        database="youtube"
    )

    cursor = cnx.cursor()

    cursor.execute(query)
    results = cursor.fetchall()

    
    cursor.close()
    cnx.close()

    return results