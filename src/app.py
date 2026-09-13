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


def init_db():
    """Initializes the database schema with retries."""
    retries = 10
    while retries > 0:
        try:
            Base.metadata.create_all(bind=engine)
            break
        except Exception:
            retries -= 1
            time.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the startup and shutdown lifespan events of the FastAPI application."""
    init_db()
    election.start_heartbeat_loop()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health(db: Session = Depends(get_db)):
    """Health check endpoint to verify DB connection and node status."""
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"

    count = db.query(Node).filter(Node.status == "active").count()

    return {
        "status": "ok",
        "db": db_status,
        "nodes_count": count,
        "node_id": election.NODE_ID,
        "leader_id": election.leader_id,
        "leader": election.leader_id,
        "is_leader": election.is_leader,
    }


@app.get("/leader")
@app.get("/api/leader")
@app.get("/election/leader")
@app.get("/api/election/leader")
def get_leader_info():
    """Endpoint GET to retrieve current leader ID and leadership status."""
    return {
        "leader_id": election.leader_id,
        "leader": election.leader_id,
        "is_leader": election.is_leader,
        "node_id": election.NODE_ID,
        "status": "ok"
    }


@app.get("/election")
@app.get("/api/election")
def get_election_info():
    """Endpoint GET to retrieve full election status of the node."""
    return election.get_election_state()


@app.post("/leader")
@app.post("/api/leader")
@app.post("/election")
@app.post("/api/election")
def handle_election_endpoint(payload: dict = None):
    """Universal POST endpoint for processing election, victory, and start messages."""
    if payload is None:
        payload = {}

    msg_type = str(payload.get("type", "")).lower()
    sender_id = payload.get("sender_id")
    leader_id = payload.get("leader_id")

    if msg_type == "start":
        election.start_election_async()
        return {"status": "election_started", "node_id": election.NODE_ID}

    if msg_type in ["victory", "coordinator", "leader"] or (leader_id is not None and msg_type != "election"):
        target_leader = leader_id if leader_id is not None else sender_id
        if target_leader is None:
            raise HTTPException(status_code=400, detail="Missing leader_id")
        return election.handle_victory_message(int(target_leader))

    if sender_id is not None or msg_type == "election":
        if sender_id is None:
            raise HTTPException(status_code=400, detail="Missing sender_id")
        return election.handle_election_message(int(sender_id))

    election.start_election_async()
    return {"status": "election_started", "node_id": election.NODE_ID}


@app.post("/election/start")
@app.post("/api/election/start")
def start_election_endpoint():
    """Dedicated endpoint to trigger an election manually."""
    election.start_election_async()
    return {"status": "election_started", "node_id": election.NODE_ID}


@app.post("/election/victory")
@app.post("/api/election/victory")
def victory_endpoint(payload: dict):
    """Dedicated endpoint to receive victory announcements from a new leader."""
    leader_id = payload.get("leader_id") or payload.get("sender_id")
    if leader_id is None:
        raise HTTPException(status_code=400, detail="Missing leader_id")
    return election.handle_victory_message(int(leader_id))


@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    """Registers a new node in the node registry database."""
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node


@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    """Lists all registered nodes in the database."""
    return db.query(Node).all()


@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    """Retrieves a specific node by name."""
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    """Updates an existing node's host or port by name."""
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node


@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    """Deactivates a node by setting status to 'inactive'."""
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)
