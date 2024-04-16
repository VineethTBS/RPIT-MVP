from celery import Celery
from conf.base import BaseConfig
from kombu import Queue
from const import taskconst
from datetime import timedelta
from kombu.serialization import register
from pfapiutils.jsonencoder import my_dumps, my_loads

## below code is used to register custom serializer
# register('myjson', my_dumps, my_loads, 
#     content_type='application/x-myjson',
#     content_encoding='utf-8') 
config = BaseConfig()
celery = Celery(config.PROJECT_NAME, broker=config.CELERY_BROKER_URL)
# Tell celery to use your custom serializer:
# celery.conf.accept_content = ['myjson']
# celery.conf.task_serializer = 'myjson'
# celery.conf.result_serializer = 'myjson'
celery.conf.task_serializer = 'pickle'
celery.conf.result_serializer = 'pickle'
celery.conf.accept_content = ['pickle']
celery.conf.task_acks_late = True 
celery.conf.task_create_missing_queues = True
celery.conf.worker_prefetch_multiplier = 1
celery.conf.broker_pool_limit = 0
celery.conf.broker_connection_timeout = 20
celery.conf.result_backend = config.CELERY_RESULT_BACKEND
celery.conf.task_queues = (
   Queue(taskconst.SCHEDULER_QUEUE),
   Queue(taskconst.FILEPROCESSOR_QUEUE)
)
celery.conf.task_routes = { f'{taskconst.FILEPROCESSOR_QUEUE}.*': {'queue': taskconst.FILEPROCESSOR_QUEUE},
                            f'{taskconst.SCHEDULER_QUEUE}.*': {'queue': taskconst.SCHEDULER_QUEUE}
                        }
celery.conf.beat_schedule = {
        'run-task-schedular': {
            'task': taskconst.SCHEDULE_TASK,
            'schedule': timedelta(hours=config.TASK_HOUR,minutes=config.TASK_MINUTE,seconds=config.TASK_SECONDS)
            #'schedule': timedelta(hours=config.TASK_HOUR,minutes=0,seconds=30)
        }
    }
