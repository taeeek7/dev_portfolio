from UploadDatabase import upload_database


upload_database("notion_database_id"  ## 노션 데이터베이스 id 입력
                , "google_sheet_key" ## 구글 API 키 입력 
                ,"spreadsheet_key" ## 구글 시트 id 입력 
                , "sheet_name" 
                , "slack_channel"
                )