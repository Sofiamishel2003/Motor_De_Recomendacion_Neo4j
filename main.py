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
    
@app.post("/node/create-single-label")
def create_single_label_node(data: dict):
    try:
        label = data.get("label")
        result = db.create_node_with_label(label)
        return {"message": "Node created successfully", "node_id": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/node/create-multiple-labels")
def create_multiple_labels_node(data: dict):
    try:
        labels = data.get("labels")
        result = db.create_node_with_multiple_labels(labels)
        return {"message": "Node created successfully", "node_id": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/node/create-with-properties")
def create_node_with_properties(data: dict):
    try:
        label = data.get("label")
        properties = data.get("properties")
        if len(properties) < 5:
            raise HTTPException(status_code=400, detail="At least 5 properties are required")
        result = db.create_node_with_properties(label, properties)
        return {"message": "Node created successfully", "node_id": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/node/add-properties")
def add_properties_to_node(data: dict):
    try:
        label = data.get("label")
        node_id = data.get("id")
        properties = data.get("properties")
        result = db.add_properties_to_node(label, node_id, properties)
        return {"message": "Properties added successfully", "node": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/nodes/{label}/add_properties")
def add_properties_to_multiple_nodes(label: str, node_ids: list[int], properties: dict):
    result = db.add_properties_to_multiple_nodes(label, node_ids, properties)
    return {"message": "Properties added successfully", "updated_nodes": result}

@app.put("/node/update-properties")
def update_node_properties(data: dict):
    try:
        label = data.get("label")
        node_id = data.get("id")
        properties = data.get("properties")
        result = db.update_node_properties(label, node_id, properties)
        return {"message": "Properties updated successfully", "node": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/nodes/{label}/update_properties")
def update_properties_multiple_nodes(label: str, node_ids: list[int], properties: dict):
    result = db.update_properties_multiple_nodes(label, node_ids, properties)
    return {"message": "Properties updated successfully", "updated_nodes": result}

@app.delete("/node/delete-properties")
def delete_node_properties(data: dict):
    try:
        label = data.get("label")
        node_id = data.get("id")
        properties = data.get("properties")
        result = db.delete_node_properties(label, node_id, properties)
        return {"message": "Properties deleted successfully", "node": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/nodes/{label}/delete_properties")
def delete_properties_multiple_nodes(label: str, node_ids: list[int], properties: list[str]):
    result = db.delete_properties_multiple_nodes(label, node_ids, properties)
    return {"message": "Properties deleted successfully", "updated_nodes": result}

@app.post("/relation/create")
def create_relation(data: dict):
    try:
        from_label = data.get("from_label")
        from_id = data.get("from_id")
        to_label = data.get("to_label")
        to_id = data.get("to_id")
        relation_type = data.get("relation_type")
        properties = data.get("properties")

        if len(properties) < 3:
            raise HTTPException(status_code=400, detail="Se requieren al menos 3 propiedades")

        result = db.create_relation(from_label, from_id, to_label, to_id, relation_type, properties)
        return {"message": "Relation created successfully", "relation": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    
