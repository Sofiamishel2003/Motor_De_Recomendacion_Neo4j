from neo4j import GraphDatabase
from pydantic import BaseModel
# Usuario
class Usuario(BaseModel):
    id: int
    nombre: str
    edad: int
    pais: str
    suscripcion: str
    ultima_fecha_vista: str
    dispositivo: str
    activo: bool
    intereses: list

# Pelicula 
class Pelicula(BaseModel):
    id: int
    titulo: str
    año: int
    duracion: float
    rating: float
    sinopsis: str
    activo: bool

# Serie
class Serie(BaseModel):
    id: int
    titulo: str
    temporadas: int
    episodios: int
    rating: float
    sinopsis: str
    activo: bool

# Genero
class Genero(BaseModel):
    id: int
    nombre: str
    popularidad: int
    descripcion: str
    subgeneros: list
    activo: bool
    
# Actor
class Actor(BaseModel):
    id: int
    nombre: str
    nacionalidad: str
    edad: int
    premios: int
    activo: bool
    
# Director
class Director(BaseModel):
    id: int
    nombre: str
    nacionalidad: str
    edad: int
    premios: int
    activo: bool

class GraphDB:
    ## BASIC
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()
    
    def _execute_query(self, query, **kwargs):
        with self.driver.session() as session:
            rslt = session.run(query, **kwargs)
            records = [dict(record) for record in rslt]
            formatted_results = [
                {key: dict(value) if hasattr(value, "__dict__") or isinstance(value, dict) else value for key, value in record.items()}
                for record in records
            ]
            return formatted_results
    ## READ
    def read_1_node(self, label, id):
        query = "MATCH (n:"+label+" {id: $id}) RETURN n LIMIT 1"
        return self._execute_query(query, id=id)
    
    def count_relations(self, rel, id, label, from_or_to):
        id_str = "{id: $id}"
        query = ""
        if from_or_to: # True = Van de nuestro nodo a cualquier otro
            query = f"MATCH (u:{label} {id_str})-[r:{rel}]->() RETURN count(r) as rel_counted"
        else: # False = Llegan a nuestro nodo desde cualquier otro
            query = f"MATCH (u:{label} {id_str})<-[r:{rel}]-() RETURN count(r) as rel_counted"
        return self._execute_query(query, id=id)
    
    ## CREATE
    def create_1_node(self, node):
        with self.driver.session() as session:
            label = node.__class__.__name__
            fields = list(node.model_fields.keys())
            query = "MERGE(:"+label+" {"
            for index, field in enumerate(fields):
                query+= field+": $"+field
                if (index !=len(fields)-1):
                    query+=", " 
            query+= "})"
            params = node.model_dump()
            session.run(query, params)

        
    ## REL / VIO
    def c_rel_vio(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Usuario)-[:VIO]->(:Pelicula) 
            session.run("""
                MATCH (a:Usuario {id: $from_n}),(b:Pelicula {id: $to_n})
                MERGE (a)-[:VIO {
                    fecha: date($fecha), 
                    dispositivo: $dispositivo, 
                    rating: toFloat($rating)
                }]->(b)""", 
                from_n=int(from_n), to_n =int(to_n), 
                fecha=props[2], dispositivo=props[3], rating=props[4]
            )
            
    ## REL / CALIFICO
    def c_rel_cal(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Usuario)-[:CALIFICO]->(:Pelicula)
            session.run("""
                MATCH (a:Usuario {id: $from_n}),(b:Pelicula {id: $to_n})
                MERGE (a)-[:CALIFICO {
                    fecha: date($fecha), 
                    calificacion: toFloat($calificacion), 
                    comentario: $comentario
                }]->(b)""",
                from_n=int(from_n), to_n =int(to_n), 
                fecha=props[2], calificacion=props[3], comentario=props[4]
            )
            
    ## REL / RECOMENDO
    def c_rel_rec(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Usuario)-[:RECOMENDO]->(:Pelicula) 
            session.run("""
                MATCH (a:Usuario {id: $from_n}),(b:Pelicula {id: $to_n})
                MERGE (a)-[:RECOMENDO {
                    fecha: date($fecha), 
                    razon: $razon, 
                    confianza: toFloat($confianza)
                }]->(b)""", 
                from_n=int(from_n), to_n =int(to_n), 
                fecha=props[2], razon=props[3], confianza=props[4]
            )
    ## REL / SIGUE
    def c_rel_sig(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Usuario)-[:SIGUE]->(:Director)  
            session.run("""
                MATCH (a:Usuario {id: $from_n}),(b:Director {id: $to_n})
                MERGE (a)-[:SIGUE {
                    fecha_inicio: date($fecha_inicio), 
                    nivel_interes: toInteger($nivel_interes), 
                    notificaciones: toBoolean($notificaciones)
                }]->(b)""", 
                from_n=int(from_n), to_n =int(to_n), 
                fecha_inicio=props[2],  nivel_interes=props[3], notificaciones=props[4]
                )
    ## REL / ADMIRA
    def c_rel_adm(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Usuario)-[:ADMIRA]->(:Actor)
            session.run("""
                MATCH (a:Usuario {id: $from_n}),(b:Actor {id: $to_n})
                MERGE (a)-[:ADMIRA {
                    fecha_inicio: date($fecha_inicio), 
                    nivel_admiracion: toInteger($nivel_admiracion), 
                    razon: $razon
                }]->(b)""", 
                from_n=int(from_n), to_n =int(to_n), 
                fecha_inicio=props[2],  nivel_admiracion=props[3], razon=props[4]
            )
            
    ## REL / PERTENECE_A
    def c_rel_per(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Pelicula)-[:PERTENECE_A]->(:Genero)
            session.run("""
                MATCH (a:Pelicula {id: $from_n}),(b:Genero {id: $to_n})
                MERGE (a)-[:PERTENECE_A {
                    peso: toFloat($peso), 
                    relevancia: $relevancia, 
                    fecha_asignacion: date($fecha_asignacion)
                }]->(b)""", 
                from_n=int(from_n), to_n =int(to_n), 
                peso=props[2],  relevancia=props[3], fecha_asignacion=props[4]
            ) 
    
    ## REL / TIENE_TEMATICA
    def c_rel_tem(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Serie)-[:TIENE_TEMATICA]->(:Genero) 
            session.run("""
                MATCH (a:Serie {id: $from_n}),(b:Genero {id: $to_n})
                MERGE (a)-[:TIENE_TEMATICA {
                    popularidad: toInteger($popularidad), 
                    tendencia: toBoolean($tendencia), 
                    impacto_cultural: $impacto_cultural
                }]->(b)""", 
                from_n=int(from_n), to_n =int(to_n), 
                popularidad=props[2],  tendencia=props[3], impacto_cultural=props[4]
            )
            
    ## REL / DIRIGIDA_POR
    def c_rel_dir(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Pelicula)-[:DIRIGIDA_POR]->(:Director)
            session.run("""
                MATCH (a:Pelicula {id: $from_n}),(b:Director {id: $to_n})
                MERGE (a)-[:DIRIGIDA_POR {
                    tipo: $tipo, 
                    experiencia: toInteger($experiencia), 
                    premios_ganados: toInteger($premios_ganados)
                }]->(b)""", 
                from_n=int(from_n), to_n =int(to_n), 
                tipo=props[2],  experiencia=props[3], premios_ganados=props[4]
            )
    ## REL / PRODUCIDA_POR
    def c_rel_pro(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Serie)-[:PRODUCIDA_POR]->(:Director) 
            session.run("""
                MATCH (a:Serie {id: $from_n}),(b:Director {id: $to_n})
                MERGE (a)-[:PRODUCIDA_POR {
                    tipo: $tipo, 
                    productora: $productora, 
                    años_experiencia: toInteger($años_experiencia)
                }]->(b)""",
            from_n=int(from_n), to_n =int(to_n), 
            tipo=props[2],  productora=props[3], años_experiencia=props[4]
            )
            
    ## REL / PARTICIPO_EN
    def c_rel_pro(self, from_n, to_n, props):
        with self.driver.session() as session:
            # (:Actor)-[:PARTICIPO_EN]->(:Pelicula) 
            session.run("""
                MATCH (a:Actor {id: $from_n}),(b:Pelicula {id: $to_n})
                MERGE (a)-[:PARTICIPO_EN {
                    rol: $rol, 
                    apariciones: toInteger($apariciones), 
                    premios_obtenidos: toInteger($premios_obtenidos)
                }]->(b)""", 
                from_n=int(from_n), to_n =int(to_n), 
                rol=props[2],  apariciones=props[3], premios_obtenidos=props[4]
            )