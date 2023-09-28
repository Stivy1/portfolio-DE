from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def text_to_drive(text: str, folder_id:str):
    
    service = build('drive', 'v3')

    file_metadata = {
                'name': text,
                'parents': [folder_id],
                'scopes': ['https://www.googleapis.com/auth/spreadsheets',
                            'https://www.googleapis.com/auth/drive']
            }

    media = MediaFileUpload(text, mimetype='text/csv', resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    debug = F'File with ID: "{file.get("id")}" has added to the folder with ID "{folder_id}".'

    return debug
