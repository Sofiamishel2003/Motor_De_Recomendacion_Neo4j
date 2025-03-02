from fastapi import FastAPI, HTTPException
from pydantic_settings import BaseSettings
from model import GraphDB, Usuario, Pelicula, Serie, Genero, Actor, Director
    

class Settings(BaseSettings):
    neo4j_uri: str
    neo4j_username: str
    neo4j_password: str

    class Config:
        env_file = ".env"

# Create a global settings instance
settings = Settings()
db = GraphDB(settings.neo4j_uri,settings.neo4j_username,settings.neo4j_password)

    
# cargar_datos("./data.csv")
app = FastAPI()
@app.get("/")
def home():
    return {"message": "Hello, FastAPI!"}

# Relationship Operations
@app.post("/rel-count")
def count_realtions(data: dict):
    try:
        rel= data.get("rel")
        from_id =  data.get("id")
        from_label = data.get("label")
        from_or_to = data.get("from_or_to")
        return db.count_relations_to(rel,from_id, from_label, from_or_to)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Node Operations
@app.get("/node/get-one")
def get_one_node(data: dict):
    try:
        label= data.get("label")
        id =  data.get("id")
        return db.read_1_node(label,id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Create
# ------- Usuario ------- #
@app.post("/user")
def create_user(user: Usuario):
    try:
        db.create_1_node(user)
        return {"message": "User created succesfully", "id": user.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ------- Pelicula ------- #
@app.post("/movie")
def create_user(movie: Pelicula):
    try:
        db.create_1_node(movie)
        return {"message": "Movie created successfully", "id": movie.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ------- Serie ------- #
@app.post("/serie")
def create_user(serie: Serie):
    try:
        db.create_1_node(serie)
        return {"message": "Series created successfully", "id": serie.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ------- Genero ------- #
@app.post("/genre")
def create_user(genre: Genero):
    try:
        db.create_1_node(genre)
        return {"message": "Genre created successfully", "id": genre.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ------- Actor ------- #
@app.post("/actor")
def create_user(actor: Actor):
    try:
        db.create_1_node(actor)
        return {"message": "Actor created successfully", "id": actor.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ------- Director ------- #
@app.post("/director")
def create_user(director: Director):
    try:
        db.create_1_node(director)
        return {"message": "Director created successfully", "id": director.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    