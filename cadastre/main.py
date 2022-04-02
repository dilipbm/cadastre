import logging
import tempfile
from uuid import uuid4
from pathlib import Path

from fastapi import FastAPI, File, UploadFile, status
from fastapi.responses import JSONResponse, FileResponse
from celery.result import AsyncResult
from celery import states
import pandas as pd

from cadastre.tasks import get_parcelles as task_get_parcelles
from cadastre.tasks import app as celery_app
from cadastre import __version__
from cadastre.utils import ContentType
from cadastre.models import Message

app = FastAPI()


@app.get("/version")
def version():
    return {"version": __version__}


@app.post(
    "/uploadfile",
    responses={
        status.HTTP_406_NOT_ACCEPTABLE: {"model": Message},
        status.HTTP_200_OK: {"model": Message},
    },
)
async def create_upload_file(file: UploadFile = File(...)) -> JSONResponse:

    """This endpoint is responsible to managing uploaded CSV file to the server

    Args:
        response (Response): Response object
        file (UploadFile, optional): CSV file. Defaults to File(...).

    Returns:
        JSONResponse: API JSON response
    """
    if file.content_type == ContentType.CSV.value:
        tmp_dir = Path(tempfile.gettempdir())
        filename = f"{uuid4()}.csv"
        tmp_filename = tmp_dir.joinpath(filename)
        with open(tmp_filename, "wb") as fout:
            file.file.seek(0)
            fout.write(file.file.read())
            fout.flush()

        file.file.seek(0)
        df = pd.read_csv(file.file)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "file uploaded",
                "filename": filename,
                "columns": df.columns.to_list(),
            },
        )
    else:
        return JSONResponse(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            content={"message": "csv file required"},
        )


@app.get("/getParcelles/", responses={status.HTTP_200_OK: {"model": Message}})
async def get_parcelles(
    filename: str, latcolname: str, lgtcolname: str, seperator: str = ";"
) -> JSONResponse:
    """This endpoint is responsible to start process with the given csv file name

    Args:
        filename (str): csv filename
        latcolname (str): latitude column name
        lgtcolname (str): longtitude column name

    Returns:
        JSONResponse: API JSON response
    """
    tmp_dir = Path(tempfile.gettempdir())
    task = task_get_parcelles.delay(
        str(tmp_dir.joinpath(filename)),
        lat_col_name=latcolname,
        lgt_col_name=lgtcolname,
        sep=seperator,
    )
    return {"message": "task started", "task_id": task.id}


@app.get("/downloadResult/{task_id}")
def download_file(task_id: str) -> JSONResponse:
    """This endpoint is responsible to download a terminated file from given task id

    Args:
        task_id (str): Celery task ID

    Returns:
        JSONResponse: API JSON response
    """

    res = AsyncResult(id=task_id, app=celery_app)
    if res.ready():
        if res.state == states.SUCCESS:
            return FileResponse(
                path=res.result.get("filename"),
                filename="données_cadastres.csv",
                media_type=ContentType.CSV.value,
            )
        elif res.state == states.FAILURE:
            logging.error(f"Task ID {task_id} failed")
            logging.error(res.info)
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"message": "process terminated with error"},
            )
    else:
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"message": "file not ready yet"}
        )
