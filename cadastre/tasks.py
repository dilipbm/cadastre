from asyncio.log import logger
from io import StringIO
from pathlib import Path
from typing import Dict
from celery import Celery
import os
import pandas as pd
import json
import requests
from uuid import uuid4

from cadastre.services import store_file
from cadastre.services import read_file_to_df


UPLOAD_FOLDER = "cadastreapi_tmp_storage"

app = Celery(__name__)
app.conf.update(
    BROKER_URL=os.environ["REDISCLOUD_URL"],
    CELERY_RESULT_BACKEND=os.environ["REDISCLOUD_URL"],
)


@app.task(name="get_parcelles")
def get_parcelles(file_: str, lat_col_name: str, lgt_col_name: str, sep: str) -> Dict:
    base_url = "https://apicarto.ign.fr/api/cadastre/parcelle"
    parcelles_data = []
    logger.debug(f"handling filename {file_}")
    df = read_file_to_df(filename=file_, sep=sep)
    if lat_col_name not in df.columns:
        raise ValueError(f"column name <{lat_col_name}> is missing in csv file")

    if lgt_col_name not in df.columns:
        raise ValueError(f"column name <{lgt_col_name}> is missing in csv file")

    for row in df.itertuples():
        lat = row.__getattribute__(lat_col_name)
        lon = row.__getattribute__(lgt_col_name)
        geom = {"type": "Point", "coordinates": [lat, lon]}
        url = f"{base_url}?_limit=1&geom={json.dumps(geom)}"
        response = requests.get(url)
        if response.status_code == 200:
            try:
                feature = response.json().get("features")[0]
            except IndexError:
                feature = None
            if feature:
                properties = feature.get("properties")
                row_dict = row._asdict()

                try:
                    row_dict.pop("Index")
                except KeyError:
                    logger.debug("row not contain Index key")

                parcell_data = {
                    **row_dict,
                    "id": feature.get("id"),
                    "numero": properties.get("numero"),
                    "feuille": properties.get("feuille"),
                    "section": properties.get("section"),
                    "code_dep": properties.get("code_dep"),
                    "code_com": properties.get("code_com"),
                    "com_abs": properties.get("com_abs"),
                    "echelle": properties.get("echelle"),
                    "code_arr": properties.get("code_arr"),
                }
                parcelles_data.append(parcell_data)
                continue

        parcell_data = {**row_dict, "id": "cadastre data not found for this geometry"}
        parcelles_data.append(parcell_data)

    df_out = pd.DataFrame.from_records(parcelles_data)
    upload_dir = Path(UPLOAD_FOLDER)
    temp_filename = f"{uuid4()}_out.csv"
    tmp_output_file = upload_dir.joinpath(temp_filename)
    tmp_output_file_buffer = StringIO()
    df_out.to_csv(tmp_output_file_buffer, index=False, sep=";")
    store_file(filename=str(tmp_output_file), content=tmp_output_file_buffer.getvalue())

    total_line = len(df_out)
    success = len(df_out[~df_out["numero"].isnull()])
    failure = len(df_out[df_out["numero"].isnull()])
    if total_line > 0:
        return {
            "filename": str(tmp_output_file),
            "success_rate": success / total_line,
            "failure_rate": failure / total_line,
        }
    else:
        return {
            "filename": str(tmp_output_file),
            "success_rate": "NA",
            "failure_rate": "NA",
        }
