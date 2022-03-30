from xml.sax.handler import feature_validation
from fastapi import FastAPI, File, UploadFile
import pandas as pd
import http3
import json
import requests

app = FastAPI()
apicarto_client = http3.AsyncClient()


async def call_api(url: str):
    r = await apicarto_client.get(url)
    return r.text


@app.get("/parcelles")
def get_parcelles():
    return {"message": "OK"}


@app.post("/uploadfile")
async def create_upload_file(file: UploadFile = File(...)):
    parcelles_data = []
    if file.content_type == "text/csv":
        df = pd.read_csv(file.file)
        for row in df.itertuples():
            lat = row.latitude
            lon = row.longtitude
            geom = {"type": "Point", "coordinates": [lat, lon]}
            url = (
                "https://apicarto.ign.fr/api/cadastre/parcelle?_limit=1&geom="
                + json.dumps(geom)
            )
            response = requests.get(url)
            if response.status_code == 200:
                try:
                    feature = response.json().get("features")[0]
                except KeyError:
                    feature = None

                if feature:
                    properties = feature.get("properties")
                    parcell_data = {
                        "lat": lat,
                        "lon": lon,
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
                else:
                    parcell_data = {"lat": lat, "lon": lon, "id": "not found"}

                parcelles_data.append(parcell_data)

    return {"data": parcelles_data}

