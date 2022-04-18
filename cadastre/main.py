from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI

from cadastre import __version__
from cadastre.routers import parcelle

UPLOAD_FOLDER = "cadastreapi_tmp_storage"
app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(parcelle.router)


@app.get("/version")
def version():
    return {"version": __version__}
