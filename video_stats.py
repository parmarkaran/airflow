import requests
import json
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv(dotenv_path="./.env")

api_key = os.getenv("API_KEY")
channel_handle = "MrBeast"
maxResults = 50

def get_playlist_id():

    try:
                
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

        response = requests.get(url)

        response.raise_for_status()

        data = response.json()

        # print(data)

        # print(json.dumps(data, indent=4))

        channel_items = data['items'][0]

        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']

        print(channel_playlistId)

        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e
def get_video_ids(playlistId):
    video_ids = []
    pageToken = None
    base_url  = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={api_key}"

    try:
        while True:
                url = base_url

                if pageToken:
                    url += f"&pageToken={pageToken}"

                response = requests.get(url)

                response.raise_for_status()

                data = response.json()

                for item in data.get('items', []):
                    video_id = item['contentDetails']['videoId']
                    video_ids.append(video_id)

                pageToken = data.get('nextPageToken')

                if not pageToken:
                    break

    except requests.exceptions.RequestException as e:
        raise e

    return video_ids


        
def extract_video_data(video_ids):
    # 1. Safety Check: If video_ids is None or empty, stop immediately
    if not video_ids:
        print("No video IDs to process.")
        return []

    extracted_data = []
    
    def batch_list(video_id_list, batch_size):
        for i in range(0, len(video_id_list), batch_size):
            yield video_id_list[i:i + batch_size]

    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_string = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_string}&key={api_key}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                statistics = item['statistics']
                contentDetails = item['contentDetails']
                
                # 2. Fix KeyError: Use .get() method with a default value of 0
                # This prevents crashing if likes or comments are disabled
                video_data = {
                    'video_id': video_id,
                    'title': snippet['title'],
                    'published_at': snippet['publishedAt'],
                    'view_count': statistics.get('viewCount', 0),
                    'like_count': statistics.get('likeCount', 0),
                    'comment_count': statistics.get('commentCount', 0),
                    'duration': contentDetails['duration']
                }
                extracted_data.append(video_data)
                
        return extracted_data

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return [] # Return empty list on error instead of crashing entirely if you prefer


def save_to_json(extracted_data):
    file_path = f"./data/airflow{date.today()}.json"
    with open(file_path, 'w', encoding='utf-8') as json_outfiles: #w for write mode
        json.dump(extracted_data, json_outfiles, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    # print("Fetching playlist ID...")
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    print(extract_video_data(video_ids))
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
    