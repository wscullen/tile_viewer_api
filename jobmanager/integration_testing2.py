from .settings import *


DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql_psycopg2",
        "NAME": "jobmanagerapp",  # prevvalue = jobmanagerapp
        "USER": "jobmanagerapp",  # prevvalue = jobmanagerapp
        "PASSWORD": "JumpMan85",  # prevvale = os.environ['JOBMANAGER_DB_PASSWORD'],
        "HOST": DEVELOPMENT_URL,
        "PORT": "5433",
    }
}
