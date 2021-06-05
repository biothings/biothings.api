SQL_URI = 'mysql+pymysql://root@localhost/album'
SQL_TBLS = {
    None: 'album',
    'album': 'album',
    'track': 'album JOIN track ON album.id = track.album_id'
}

API_PREFIX = "sql"
