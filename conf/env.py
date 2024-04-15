import os

from .localconf import LocalConfig
from .stage import StagingConfig
#from prod import ProdConfig

def get_config():
   SWITCH = {
       'local'     : LocalConfig,
       'stage'   : StagingConfig
       #,'prod': ProdConfig
   }
   if os.environ.get('APP_ENV'):
      print('APP_ENV :: ' + os.environ.get('APP_ENV'))
      return SWITCH[os.environ.get('APP_ENV')]
   else:
      return SWITCH['local'] # default to LOCAL