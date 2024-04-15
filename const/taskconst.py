#queue names
SCHEDULER_QUEUE = 'scheduler'
FILEPROCESSOR_QUEUE = 'fileprocessor'

#background task names
PROCESS_FILE_TASK = f'{FILEPROCESSOR_QUEUE}.processfile'
DOWNLOAD_FILE_TASK = f'{FILEPROCESSOR_QUEUE}.downloadfile'
GET_FILES_LIST_SFTP_TASK = f'{FILEPROCESSOR_QUEUE}.get_files_for_processing'
SCHEDULE_TASK = f'{SCHEDULER_QUEUE}.scheduletask'
POST_PROCESSING_TASK_BG = f'{FILEPROCESSOR_QUEUE}.do_postprocessing_background'
POST_PROCESSING_TASK_FG = f'{FILEPROCESSOR_QUEUE}.do_postprocessing_foreground'
RETRY_FAILED_PROCESSFILE_TASK= f'{FILEPROCESSOR_QUEUE}.processfile_retry'
ADD_JOB = f'{FILEPROCESSOR_QUEUE}.addjob'
UPDATE_JOB = f'{FILEPROCESSOR_QUEUE}.updatejob'
GET_JOB = f'{FILEPROCESSOR_QUEUE}.getjob'

#job type
PROCESS_FILE = "import_file_data"
