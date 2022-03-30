from celery import Celery
import os

app = Celery(__name__)
app.conf.update(
    BROKER_URL=os.environ["REDISCLOUD_URL"],
    CELERY_RESULT_BACKEND=os.environ["REDISCLOUD_URL"]
)


@app.task(name="get_parcelles")
def get_parcelles(username, password, query, n_pages):
    results = "OK"
    return results