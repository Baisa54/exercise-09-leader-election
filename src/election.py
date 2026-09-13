"""
Bully algorithm for leader election across Node Registry instances.

Functions implemented:
- start_election(): initiate an election, send ELECTION messages to higher-ID nodes
- handle_election_message(sender_id): respond to election from lower-ID node
- declare_victory(): announce self as leader to all nodes
- heartbeat_check(): periodically check if leader is alive
"""

# Importación de módulos del sistema y librerías estándar
import os          # Permite leer las variables de entorno del sistema (como NODE_ID y PEERS)
import time        # Proporciona funciones de temporización (como sleep) para timeouts y retardos
import threading   # Proporciona soporte para hilos (threads) y cerrojos (Locks) para sincronización concurrente
import requests    # Librería HTTP para realizar peticiones (GET, POST) a otros nodos peers
from typing import Optional, List, Dict, Any  # Importación de tipos para sugerencias de tipado (Type Hints) en funciones

# Variable global que obtiene el ID único del nodo actual desde la variable de entorno 'NODE_ID'.
NODE_ID: int = int(os.environ.get("NODE_ID", 1))

# Variable global que obtiene la lista de URLs de nodos pares (peers) separada por comas desde la variable de entorno 'PEERS'.
PEERS_ENV: str = os.environ.get("PEERS", "")

# Estado global del nodo en el algoritmo de elección
leader_id: Optional[int] = None
is_leader: bool = False
in_election: bool = False
_lock = threading.Lock()
_peer_id_cache: Dict[str, int] = {}


def get_node_id() -> int:
    """Returns current node ID."""
    return NODE_ID


def parse_peers() -> List[str]:
    """Parses PEERS_ENV environment variable into a list of peer URLs."""
    if not PEERS_ENV:
        return []
    raw = PEERS_ENV.split(",")
    peers = [p.strip().rstrip("/") for p in raw if p.strip()]
    return peers


def get_peers() -> List[str]:
    """Returns list of peer URLs excluding self."""
    peers = parse_peers()

    # Consultar BD si existen nodos activos registrados
    try:
        from src.database import SessionLocal
        from src.models import Node

        db = SessionLocal()
        try:
            db_nodes = db.query(Node).filter(Node.status == "active").all()
            for n in db_nodes:
                url = f"http://{n.host}:{n.port}"
                if url not in peers:
                    peers.append(url)
        finally:
            db.close()
    except Exception:
        pass

    self_urls = [
        f"http://node-{NODE_ID}:8080",
        f"http://localhost:{8080 + NODE_ID}",
        f"http://127.0.0.1:{8080 + NODE_ID}"
    ]
    filtered = [p for p in peers if p not in self_urls]
    return filtered


def get_peer_id(peer_url: str) -> Optional[int]:
    """Queries peer to get its node_id or extracts from cache/URL."""
    if peer_url in _peer_id_cache:
        return _peer_id_cache[peer_url]

    try:
        if "node-" in peer_url:
            part = peer_url.split("node-")[1].split(":")[0].split("/")[0]
            if part.isdigit():
                nid = int(part)
                _peer_id_cache[peer_url] = nid
                return nid
    except Exception:
        pass

    try:
        resp = requests.get(f"{peer_url}/election", timeout=0.5)
        if resp.status_code == 200:
            data = resp.json()
            nid = data.get("node_id")
            if nid is not None:
                _peer_id_cache[peer_url] = int(nid)
                return int(nid)
    except Exception:
        pass

    return None


def handle_election_message(sender_id: int) -> Dict[str, Any]:
    """Respond to election message from lower-ID node."""
    global in_election
    print(f"[Node {NODE_ID}] Received ELECTION message from Node {sender_id}")
    if sender_id < NODE_ID:
        if not in_election:
            start_election_async()
        return {"status": "ok", "message": "OK", "node_id": NODE_ID}
    else:
        return {"status": "ignored", "message": "Sender ID is higher or equal", "node_id": NODE_ID}


def handle_victory_message(leader_node_id: int) -> Dict[str, Any]:
    """Handle victory message from new leader."""
    global leader_id, is_leader, in_election
    with _lock:
        leader_id = leader_node_id
        is_leader = (leader_node_id == NODE_ID)
        in_election = False
    print(f"[Node {NODE_ID}] Node {leader_node_id} declared victory. New leader is Node {leader_id}.")
    return {
        "status": "ok",
        "leader_id": leader_id,
        "leader": leader_id,
        "is_leader": is_leader,
        "node_id": NODE_ID
    }


def declare_victory():
    """Announce self as leader to all nodes."""
    global is_leader, leader_id, in_election
    with _lock:
        is_leader = True
        leader_id = NODE_ID
        in_election = False

    print(f"[Node {NODE_ID}] Declaring victory! I am the new leader.")

    peers = get_peers()
    for peer in peers:
        try:
            requests.post(
                f"{peer}/election",
                json={"type": "victory", "leader_id": NODE_ID, "sender_id": NODE_ID},
                timeout=0.8
            )
        except Exception:
            pass


def start_election():
    """Initiate an election, send ELECTION messages to higher-ID nodes."""
    global in_election, leader_id, is_leader
    with _lock:
        if in_election:
            return
        in_election = True
        leader_id = None
        is_leader = False

    print(f"[Node {NODE_ID}] Starting leader election...")

    peers = get_peers()
    higher_nodes_responded = False

    for peer in peers:
        peer_id = get_peer_id(peer)
        if peer_id is not None and peer_id <= NODE_ID:
            continue

        try:
            resp = requests.post(
                f"{peer}/election",
                json={"type": "election", "sender_id": NODE_ID},
                timeout=0.8
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "ok":
                    higher_nodes_responded = True
        except Exception:
            pass

    if not higher_nodes_responded:
        declare_victory()
    else:
        def election_timeout_check():
            time.sleep(2.0)
            global in_election, leader_id
            if leader_id is None:
                with _lock:
                    in_election = False
                start_election_async()

        threading.Thread(target=election_timeout_check, daemon=True).start()


def start_election_async():
    """Helper to start election in a background thread."""
    threading.Thread(target=start_election, daemon=True).start()


def find_leader_url(target_leader_id: int) -> Optional[str]:
    """Finds peer URL corresponding to target_leader_id."""
    peers = get_peers()
    for peer in peers:
        nid = get_peer_id(peer)
        if nid == target_leader_id:
            return peer

    for peer in peers:
        if f"node-{target_leader_id}" in peer:
            return peer

    return f"http://node-{target_leader_id}:8080"


def heartbeat_check():
    """Periodically check if leader is alive."""
    global leader_id, is_leader, in_election
    if is_leader:
        return

    if leader_id is None:
        if not in_election:
            start_election_async()
        return

    leader_url = find_leader_url(leader_id)
    if not leader_url:
        print(f"[Node {NODE_ID}] Could not determine leader URL for Node {leader_id}. Triggering election.")
        leader_id = None
        if not in_election:
            start_election_async()
        return

    try:
        resp = requests.get(f"{leader_url}/health", timeout=0.8)
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code}")
    except Exception:
        print(f"[Node {NODE_ID}] Leader {leader_id} at {leader_url} is unreachable. Triggering election...")
        leader_id = None
        if not in_election:
            start_election_async()


def heartbeat_loop():
    """Background loop that continuously runs heartbeat checks at set intervals."""
    if leader_id is None and not in_election:
        start_election_async()

    while True:
        try:
            heartbeat_check()
        except Exception as e:
            print(f"[Node {NODE_ID}] Heartbeat error: {e}")
        time.sleep(1.5)


def start_heartbeat_loop():
    """Starts the heartbeat monitoring loop in a background daemon thread."""
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()


def get_election_state() -> Dict[str, Any]:
    """Returns current election state for node."""
    return {
        "node_id": NODE_ID,
        "leader_id": leader_id,
        "leader": leader_id,
        "is_leader": is_leader,
        "in_election": in_election,
        "peers": get_peers()
    }
