from io import StringIO
import logging
from uuid import uuid4
from pathlib import Path

from fastapi import FastAPI, APIRouter, File, Form, UploadFile, status
from fastapi.responses import JSONResponse, StreamingResponse
from celery.result import AsyncResult
from celery import states
import pandas as pd

from cadastre.tasks import get_parcelles as task_get_parcelles
from cadastre.tasks import app as celery_app
from cadastre.utils import ContentType
from cadastre.models import Message
from cadastre.models import ParcellCeleryResult
from cadastre.services import store_file, read_file, delete_file

UPLOAD_FOLDER = "cadastreapi_tmp_storage"

router = APIRouter(
    prefix="/parcelle",
    tags=["parcelle"],
)


@router.post(
    "/uploadfile",
    responses={
        status.HTTP_406_NOT_ACCEPTABLE: {"model": Message},
        status.HTTP_200_OK: {"model": Message},
    },
)
async def create_upload_file(
    file: UploadFile = File(...), seperator: str = Form(...)
) -> JSONResponse:

    """This endpoint is responsible to managing uploaded CSV file to the server

    Args:
        response (Response): Response object
        file (UploadFile, optional): CSV file. Defaults to File(...).

    Returns:
        JSONResponse: API JSON response
    """
    if (
        file.content_type == ContentType.CSV.value
        or file.content_type == ContentType.EXCEL.value
    ):
        upload_dir = Path(UPLOAD_FOLDER)
        filename = f"{uuid4()}_in.csv"
        tmp_filename = upload_dir.joinpath(filename)
        file.file.seek(0)
        result = store_file(
            str(tmp_filename), file.file.read().decode(encoding="utf-8")
        )
        if result:
            logging.info(f"file successfully uploaded")
        file.file.seek(0)
        df = pd.read_csv(file.file, sep=seperator)
        columns = df.columns.to_list()

        if len(columns) < 2:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"message": "atleast two columns required"},
            )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "file uploaded",
                "filename": filename,
                "filePath": str(tmp_filename),
                "columns": columns,
            },
        )
    else:
        return JSONResponse(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            content={"message": "csv file required"},
        )


@router.get("/getParcelles/", responses={status.HTTP_200_OK: {"model": Message}})
async def get_parcelles(
    filename: str, latcolname: str, lgtcolname: str, seperator
) -> JSONResponse:
    """This endpoint is responsible to start process with the given csv file name

    Args:
        filename (str): csv filename
        latcolname (str): latitude column name
        lgtcolname (str): longtitude column name

    Returns:
        JSONResponse: API JSON response
    """
    upload_dir = Path(UPLOAD_FOLDER)
    task = task_get_parcelles.delay(
        str(upload_dir.joinpath(filename)),
        lat_col_name=latcolname,
        lgt_col_name=lgtcolname,
        sep=seperator,
    )
    return {"message": "task started", "task_id": task.id}


@router.get("/downloadResult/{task_id}")
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
            celery_result = ParcellCeleryResult(**res.result)
            filename = celery_result.output_filename
            try:
                result_file = read_file(filename=filename)
            except:
                result_file = None

            if not result_file:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND, content={"message": "error"}
                )

            response = StreamingResponse(
                content=iter([StringIO(result_file).getvalue()]), media_type="text/csv"
            )
            response.headers[
                "Content-Disposition"
            ] = "attachment; filename=donn??es cadastre.csv"

            return response

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


@router.get("/deleteFiles/{task_id}")
def delete_files(task_id: str) -> JSONResponse:
    """This endpoint is responsible to delete all files related to given task id

    Args:
        task_id (str): Task ID

    Returns:
        JSONResponse: Return message
    """
    res = AsyncResult(id=task_id, app=celery_app)
    if res.ready():
        celery_result = ParcellCeleryResult(**res.result)
        try:
            delete_file(celery_result.input_filename)
            delete_file(celery_result.output_filename)
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"message": "files are deleted"},
            )
        except:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"message": "something went wrong"},
            )
