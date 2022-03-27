from fastapi import FastAPI

app = FastAPI()

@app.get("/parcelles")
def get_parcelles():
    return {
        "message": "OK"
    }


