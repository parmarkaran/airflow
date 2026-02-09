import logging
logger = logging.getLogger(__name__)

table = "yt_api"

def insert_rows(cur, conn, schema, row):
    try:
        if schema == 'staging':
            # STAGING SCHEMA: Insert or Update if exists
            cur.execute(f"""
                INSERT INTO {schema}.{table} (
                    "video_id",
                    "VIDEO_title",
                    "upload_date",
                    "duration",
                    "VIDEO_VIEWS",
                    "LIKES_COUNT",
                    "COMMENTS_COUNT"
                ) VALUES (
                    %(video_id)s,
                    %(title)s,
                    %(published_at)s,
                    %(duration)s,
                    %(view_count)s,
                    %(like_count)s,
                    %(comment_count)s
                )
                ON CONFLICT ("video_id") DO UPDATE SET
                    "VIDEO_title" = EXCLUDED."VIDEO_title",
                    "VIDEO_VIEWS" = EXCLUDED."VIDEO_VIEWS",
                    "LIKES_COUNT" = EXCLUDED."LIKES_COUNT",
                    "COMMENTS_COUNT" = EXCLUDED."COMMENTS_COUNT",
                    "upload_date" = EXCLUDED."upload_date";
            """, row)
            
        else:
            # CORE SCHEMA: Insert or Update if exists
            cur.execute(f"""
                INSERT INTO {schema}.{table} (
                    "video_id",
                    "VIDEO_title",
                    "upload_date",
                    "duration",
                    "VIDEO_TYPE",
                    "VIDEO_VIEWS",
                    "LIKES_COUNT",
                    "COMMENTS_COUNT"
                ) VALUES (
                    %(video_id)s,
                    %(video_title)s,
                    %(upload_date)s,
                    %(duration)s,
                    %(video_type)s,
                    %(video_views)s,
                    %(likes_count)s,
                    %(comments_count)s
                )
                ON CONFLICT ("video_id") DO UPDATE SET
                    "VIDEO_title" = EXCLUDED."VIDEO_title",
                    "VIDEO_VIEWS" = EXCLUDED."VIDEO_VIEWS",
                    "LIKES_COUNT" = EXCLUDED."LIKES_COUNT",
                    "COMMENTS_COUNT" = EXCLUDED."COMMENTS_COUNT",
                    "VIDEO_TYPE" = EXCLUDED."VIDEO_TYPE";
            """, row)
        
        conn.commit()
        logger.info(f"Upserted row for video_id: {row['video_id']} ")
    
    except Exception as e:
        logger.error(f"Error upserting row for video_id: {row.get('video_id', 'unknown')}")
        raise e
    
def update_rows(cur, conn, schema, row):
    try:
        # Define variable names based on JSON keys
        if schema == 'staging':
            video_id_key = 'video_id'
            upload_date_key = 'published_at'
            
            # Map JSON keys to expected Dictionary keys
            # Note: We don't change the SQL column names here, only the %(key)s mapping
            row['video_title'] = row['title'] 
            row['video_views'] = row['view_count']
            row['likes_count'] = row['like_count']
            row['comments_count'] = row['comment_count']
            
        else:
            video_id_key = 'video_id'
            upload_date_key = 'upload_date'

        # Use quotes here too!
        cur.execute(f"""UPDATE {schema}.{table}
            SET 
                "VIDEO_title" = %(video_title)s,
                "VIDEO_VIEWS" = %(video_views)s,
                "LIKES_COUNT" = %(likes_count)s,
                "COMMENTS_COUNT" = %(comments_count)s
            WHERE "video_id" = %({video_id_key})s AND "upload_date" = %({upload_date_key})s;""", row)
        
        conn.commit()
        logger.info(f"Updated row for video_id: {row['video_id']} ")
    
    except Exception as e:
        logger.error(f"Error updating row for video_id: {row.get('video_id', 'unknown')}")
        raise e

def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        if not ids_to_delete:
            return 

        ids_formatted = ', '.join(f"'{id}'" for id in ids_to_delete)
        
        # Use quotes here too!
        cur.execute(f"""DELETE FROM {schema}.{table} WHERE "video_id" IN ({ids_formatted});""")
    
        conn.commit()
        logger.info(f"Deleted rows for video_ids: {ids_to_delete} ")
    
    except Exception as e:
        logger.error(f"Error deleting rows: {e}")
        raise e