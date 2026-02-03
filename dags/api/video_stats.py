import requests
import json
import os
from datetime import date
from airflow.decorators import task

# ---------------------------------------------------------
# ✅ CONFIGURATION (Enter your details here)
# ---------------------------------------------------------
# We define these as global strings so all tasks can see them.
# No more "Variable.get" errors.
API_KEY = "AIzaSyAUZYKiiXFQuOuyQdr3fsCowecdZjz8_8A"
CHANNEL_HANDLE = "@MrBeast"  # Must include the @ symbol
MAX_RESULTS = 50
# ---------------------------------------------------------

# Helper to get the correct absolute path for Airflow
def get_data_path(filename):
    airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
    data_dir = os.path.join(airflow_home, 'data')
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, filename)

@task
def get_playlist_id():
    print(f"Fetching playlist for handle: {CHANNEL_HANDLE}")

    # Use 'params' to automatically handle the '@' symbol encoding
    url = "https://youtube.googleapis.com/youtube/v3/channels"
    query_params = {
        "part": "contentDetails",
        "forHandle": CHANNEL_HANDLE, 
        "key": API_KEY
    }

    try:
        response = requests.get(url, params=query_params)
        
        # If the API fails, print the REAL error message from Google
        if response.status_code != 200:
            print(f"❌ API FAILED. Status: {response.status_code}")
            print(f"❌ Google Error Message: {response.text}")
            response.raise_for_status()

        data = response.json()
        
        if not data.get('items'):
            print(f"❌ No channel found. Check if '{CHANNEL_HANDLE}' is correct.")
            raise ValueError("Channel not found")

        channel_items = data['items'][0]
        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']

        print(f"✅ Found Playlist ID: {channel_playlistId}")
        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e

@task
def get_video_ids(playlistId):
    video_ids = []
    pageToken = None
    # Use the global API_KEY variable
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlistId}&key={API_KEY}"

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

@task
def extract_video_data(video_ids):
    if not video_ids:
        print("No video IDs to process.")
        return []

    extracted_data = []
    
    def batch_list(video_id_list, batch_size):
        for i in range(0, len(video_id_list), batch_size):
            yield video_id_list[i:i + batch_size]

    try:
        for batch in batch_list(video_ids, MAX_RESULTS):
            video_ids_string = ",".join(batch)
            # Use the global API_KEY variable
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails,snippet,statistics&id={video_ids_string}&key={API_KEY}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                statistics = item['statistics']
                contentDetails = item['contentDetails']
                
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
        return []

@task
def save_to_json(extracted_data):
    # Fixed naming convention to match your first error log (airflow_YYYY-MM-DD.json)
    filename = f"airflow_{date.today()}.json"
    file_path = get_data_path(filename)
    
    print(f"Saving data to: {file_path}")
    
    with open(file_path, 'w', encoding='utf-8') as json_outfiles:
        json.dump(extracted_data, json_outfiles, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    # Local testing block
    try:
        playlistId = get_playlist_id()
        video_ids = get_video_ids(playlistId)
        video_data = extract_video_data(video_ids)
        save_to_json(video_data)
        print("✅ Script finished successfully!")
    except Exception as e:
        print(f"❌ Script failed: {e}")