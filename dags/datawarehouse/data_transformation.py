import re

def parse_duration(duration_str):
    """
    Parses YouTube ISO 8601 duration (e.g., 'PT1H2M10S', 'PT40S') 
    into total seconds (integer).
    """
    if not duration_str:
        return 0
        
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
    
    if not match:
        return 0
        
    h, m, s = match.groups()
    hours = int(h) if h else 0
    minutes = int(m) if m else 0
    seconds = int(s) if s else 0
    
    return (hours * 3600) + (minutes * 60) + seconds

def transform_data(row):
    """
    Transforms the row from STAGING (DB columns) to CORE (Standardized columns).
    Input keys must match the Staging Table definition in data_utils.py.
    """
    
    # 1. Parse duration
    # Input is 'duration' (matches DB column)
    duration_seconds = parse_duration(row.get('duration', ''))
    
    # 2. Determine Video Type
    video_type = 'Shorts' if duration_seconds <= 60 else 'Video'

    # 3. Create the Core dictionary
    # LEFT SIDE: New Core Table Column Names
    # RIGHT SIDE: Exact Staging Table Column Names (from data_utils.py)
    transformed_row = {
        'video_id': row['video_id'],
        
        # Staging column is "VIDEO_title"
        'video_title': row['VIDEO_title'],           
        
        # Staging column is "upload_date"
        'upload_date': row['upload_date'],    
        
        'duration': str(duration_seconds),
        'video_type': video_type,
        
        # Staging column is "VIDEO_VIEWS"
        'video_views': row['VIDEO_VIEWS'],      
        
        # Staging column is "LIKES_COUNT"
        'likes_count': row['LIKES_COUNT'],      
        
        # Staging column is "COMMENTS_COUNT"
        'comments_count': row['COMMENTS_COUNT'] 
    }
    
    return transformed_row