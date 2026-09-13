#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#
#Imports
#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#

# Importación del módulo de tiempo para pausar ejecuciones en bucles de reintento
import time

# Importación de datetime y timezone para manipular marcas de tiempo con zona horaria UTC
from datetime import datetime, timezone

# Importación de asynccontextmanager para gestionar el ciclo de vida (lifespan) de la aplicación FastAPI
from contextlib import asynccontextmanager

# Importaciones del framework FastAPI para manejar dependencias, la app principal, excepciones HTTP y respuestas personalizadas
from fastapi import Depends, FastAPI, HTTPException, Response

# Importación de 'text' de SQLAlchemy para ejecutar consultas SQL puras (como SELECT 1)
from sqlalchemy import text

# Importación del tipo Session para el manejo de sesiones con la base de datos
from sqlalchemy.orm import Session

# Importación de la base ORM, el motor de conexión y el generador de sesiones get_db
from src.database import Base, engine, get_db

# Importación del modelo de datos de SQLAlchemy para la tabla Node
from src.models import Node

# Importación de los esquemas Pydantic para la validación y serialización de datos de entrada/salida
from src.schemas import NodeCreate, NodeResponse, NodeUpdate

# Importación del módulo del algoritmo de elección de líder (Bully)
from src import election

#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#
#init_db
#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#

def init_db():
    """Initializes the database schema with retries."""
    # Define un máximo de 10 reintentos para dar tiempo a que la base de datos PostgreSQL arranque
    retries = 10
    while retries > 0:
        try:
            # Intenta crear todas las tablas definidas en las clases de SQLAlchemy que heredan de Base
            Base.metadata.create_all(bind=engine)
            # Si se crearon con éxito, rompe el bucle
            break
        except Exception:
            # Si la conexión falla (por ejemplo, Postgres no ha terminado de iniciar), decrementa el contador de reintentos
            retries -= 1
            # Pausa de 1 segundo antes del siguiente intento
            time.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the startup and shutdown lifespan events of the FastAPI application."""
    # En el arranque de la aplicación: inicializa las tablas de la base de datos
    init_db()
    # Inicia el bucle de monitoreo del latido (heartbeat) del líder en un hilo secundario en segundo plano
    election.start_heartbeat_loop()
    # Cede el control a la aplicación FastAPI mientras está en ejecución
    yield


# Instancia principal de la aplicación FastAPI pasando el manejador del ciclo de vida (lifespan)
app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health(db: Session = Depends(get_db)):
    """Health check endpoint to verify DB connection and node status."""
    try:
        # Ejecuta una consulta SQL simple para verificar la conectividad con PostgreSQL
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        # Si la consulta falla, marca el estado de la BD como desconectado
        db_status = "disconnected"

    # Cuenta la cantidad de nodos activos registrados en la base de datos
    count = db.query(Node).filter(Node.status == "active").count()

    # Retorna un diccionario con el estado general del servicio, la BD, el conteo de nodos y la información del líder
    return {
        "status": "ok",
        "db": db_status,
        "nodes_count": count,
        "node_id": election.NODE_ID,
        "leader_id": election.leader_id,
        "is_leader": election.is_leader,
    }


@app.get("/election")
@app.get("/api/election")
def get_election_info():
    """Endpoint GET to retrieve full election status of the node."""
    # Retorna el estado completo del proceso de elección administrado en election.py
    return election.get_election_state()


@app.get("/election/leader")
@app.get("/api/election/leader")
def get_leader_info():
    """Endpoint GET to retrieve current leader ID and leadership flag."""
    # Retorna un diccionario simplificado indicando quién es el líder actual y si este nodo es el líder
    return {"leader_id": election.leader_id, "is_leader": election.is_leader}


@app.post("/election")
@app.post("/api/election")
def handle_election_endpoint(payload: dict = None):
    """Universal POST endpoint for processing election, victory, and start messages."""
    # Si no se envió cuerpo JSON en la petición, inicializa payload como un diccionario vacío
    if payload is None:
        payload = {}

    # Extrae los parámetros clave del mensaje JSON entrante
    msg_type = payload.get("type")
    sender_id = payload.get("sender_id")
    leader_id = payload.get("leader_id")

    # Caso 1: Solicitud explícita para iniciar una elección
    if msg_type == "start":
        election.start_election_async()
        return {"status": "election_started", "node_id": election.NODE_ID}

    # Caso 2: Mensaje de declaración de victoria / coordinador entrante
    if msg_type == "victory" or msg_type == "coordinator" or (leader_id is not None and msg_type != "election"):
        if leader_id is None:
            leader_id = sender_id
        if leader_id is None:
            raise HTTPException(status_code=400, detail="Missing leader_id")
        return election.handle_victory_message(int(leader_id))

    # Caso 3: Mensaje de elección entrante desde un nodo peer
    if sender_id is not None:
        return election.handle_election_message(int(sender_id))

    # Caso por defecto: Inicia una nueva elección si el mensaje no especificó tipo ni emisor
    election.start_election_async()
    return {"status": "election_started", "node_id": election.NODE_ID}


@app.post("/election/start")
@app.post("/api/election/start")
def start_election_endpoint():
    """Dedicated endpoint to trigger an election manually."""
    # Llama a la función de elección asíncrona y retorna estado iniciado
    election.start_election_async()
    return {"status": "election_started", "node_id": election.NODE_ID}


@app.post("/election/victory")
@app.post("/api/election/victory")
def victory_endpoint(payload: dict):
    """Dedicated endpoint to receive victory announcements from a new leader."""
    # Extrae el ID del líder desde 'leader_id' o 'sender_id'
    leader_id = payload.get("leader_id") or payload.get("sender_id")
    # Si no se proporcionó un ID de líder válido, responde con HTTP 400 Bad Request
    if leader_id is None:
        raise HTTPException(status_code=400, detail="Missing leader_id")
    # Procesa la victoria actualizando el estado local del nodo
    return election.handle_victory_message(int(leader_id))


@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    """Registers a new node in the node registry database."""
    # Verifica si ya existe un nodo registrado con el mismo nombre
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    # Crea la entidad de modelo SQLAlchemy
    db_node = Node(name=node.name, host=node.host, port=node.port)
    # Guarda e inserta el nuevo registro en la BD
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node


@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    """Lists all registered nodes in the database."""
    # Retorna la lista completa de nodos almacenados
    return db.query(Node).all()


@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    """Retrieves a specific node by name."""
    # Busca el nodo por nombre
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    """Updates an existing node's host or port by name."""
    # Busca el nodo a actualizar
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    # Actualiza los campos opcionales si fueron proporcionados
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    # Actualiza la marca de tiempo de modificación
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node


@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    """Deactivates a node by setting status to 'inactive'."""
    # Busca el nodo a desactivar
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    # Cambia su estado a inactivo en lugar de borrarlo físicamente
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)
