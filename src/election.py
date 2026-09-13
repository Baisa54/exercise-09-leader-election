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
# Si no está definida en el entorno, toma por defecto el valor 1 (convertido a entero).
NODE_ID: int = int(os.environ.get("NODE_ID", 1))

# Variable global que obtiene la lista de URLs de nodos pares (peers) separada por comas desde la variable de entorno 'PEERS'.
PEERS_ENV: str = os.environ.get("PEERS", "")

# Estado global del nodo en el algoritmo de elección
# leader_id: Almacena el ID numérico del nodo reconocido como líder actual. Inicialmente es None (desconocido).
leader_id: Optional[int] = None

# is_leader: Booleano que indica si el nodo actual es el líder activo del clúster (True/False).
is_leader: bool = False

# in_election: Booleano que indica si este nodo se encuentra ejecutando un proceso de elección actualmente (True/False).
in_election: bool = False

# _lock: Objeto reentrante de cerrojo (Lock) de threading para evitar condiciones de carrera (race conditions)
# al modificar las variables globales de estado desde múltiples hilos concurrentes.
_lock = threading.Lock()

# _peer_id_cache: Diccionario en memoria que almacena en caché la relación entre las URLs de los peers y su NODE_ID correspondiente.
# Evita realizar peticiones HTTP repetitivas para descubrir el ID de un peer conocido.
_peer_id_cache: Dict[str, int] = {}


def get_node_id() -> int:
    """Returns current node ID."""
    # Retorna directamente el ID del nodo actual (NODE_ID).
    # ¿Por qué está aquí?: Permite a otros módulos (como app.py) consultar el ID único de este nodo sin acceder directamente a la variable global.
    return NODE_ID


def parse_peers() -> List[str]:
    """Parses PEERS_ENV environment variable into a list of peer URLs."""
    # Comprueba si la variable de entorno PEERS_ENV está vacía o no existe.
    if not PEERS_ENV:
        # Si está vacía, devuelve una lista vacía sin realizar procesamiento adicional.
        return []

    # Divide la cadena delimitada por comas en una lista de subcadenas individuales.
    raw = PEERS_ENV.split(",")

    # Limpia cada URL eliminando espacios en blanco a los extremos y barritas diagonales finales ('/'),
    # filtrando elementos vacíos que pudieran haber resultado de comas adicionales.
    peers = [p.strip().rstrip("/") for p in raw if p.strip()]

    # Devuelve la lista procesada de URLs de nodos peers.
    return peers


def get_peers() -> List[str]:
    """Returns list of peer URLs excluding self."""
    # Obtiene las URLs configuradas explícitamente en las variables de entorno.
    peers = parse_peers()

    # Bloque try-except para consultar la base de datos local y descubrir otros nodos registrados en la tabla Node.
    try:
        # Importación tardía de SessionLocal y Node para evitar importaciones circulares en el arranque.
        from src.database import SessionLocal
        from src.models import Node

        # Instancia una sesión de base de datos SQLAlchemy.
        db = SessionLocal()
        try:
            # Consulta todos los nodos de la tabla 'nodes' cuyo estado sea 'active'.
            db_nodes = db.query(Node).filter(Node.status == "active").all()
            
            # Recorre los nodos encontrados en la base de datos.
            for n in db_nodes:
                # Construye la URL base del nodo con el formato http://{host}:{port}
                url = f"http://{n.host}:{n.port}"
                # Si la URL descubierta no está ya en la lista de peers, se añade.
                if url not in peers:
                    peers.append(url)
        finally:
            # Cierra la sesión con la base de datos para liberar conexiones del pool.
            db.close()
    except Exception:
        # Si ocurre un fallo en la consulta a la BD (por ejemplo, BD no iniciada), se ignora silenciosamente.
        pass

    # Define las variaciones de URLs que corresponden al propio nodo actual para no comunicarse consigo mismo.
    self_urls = [
        f"http://node-{NODE_ID}:8080",
        f"http://localhost:{8080 + NODE_ID}",
        f"http://127.0.0.1:{8080 + NODE_ID}"
    ]

    # Filtra la lista reteniendo únicamente las URLs que NO coincidan con ninguna de las URLs de este nodo.
    filtered = [p for p in peers if p not in self_urls]

    # Devuelve la lista final de URLs de peers excluyendo al nodo actual.
    return filtered


def get_peer_id(peer_url: str) -> Optional[int]:
    """Queries peer to get its node_id or extracts from cache/URL."""
    # Si el ID del peer ya se encuentra guardado en el diccionario caché _peer_id_cache, se retorna inmediatamente.
    if peer_url in _peer_id_cache:
        return _peer_id_cache[peer_url]

    # Intento 1: Extraer el ID analizando el nombre de host de la URL si sigue el patrón estándar 'node-X' (ej. http://node-2:8080)
    try:
        if "node-" in peer_url:
            # Aísla la parte numérica tras 'node-' y antes del puerto o barra.
            part = peer_url.split("node-")[1].split(":")[0].split("/")[0]
            # Si el valor aislado es enteramente numérico, lo convierte a entero y lo guarda en caché.
            if part.isdigit():
                nid = int(part)
                _peer_id_cache[peer_url] = nid
                return nid
    except Exception:
        pass

    # Intento 2: Realizar una petición HTTP GET al endpoint /election del peer objetivo
    try:
        resp = requests.get(f"{peer_url}/election", timeout=1.5)
        # Si la respuesta HTTP es exitosa (código 200 OK)
        if resp.status_code == 200:
            # Extrae el atributo 'node_id' del objeto JSON de respuesta.
            nid = resp.json().get("node_id")
            if nid is not None:
                # Almacena el valor recuperado en el diccionario de caché y lo retorna.
                _peer_id_cache[peer_url] = int(nid)
                return int(nid)
    except Exception:
        pass

    # Intento 3: Realizar una petición HTTP GET al endpoint /health del peer objetivo como alternativa
    try:
        resp = requests.get(f"{peer_url}/health", timeout=1.5)
        if resp.status_code == 200:
            nid = resp.json().get("node_id")
            if nid is not None:
                _peer_id_cache[peer_url] = int(nid)
                return int(nid)
    except Exception:
        pass

    # Si no fue posible determinar el ID del peer por ninguna de las vías anteriores, devuelve None.
    return None


def handle_election_message(sender_id: int) -> Dict[str, Any]:
    """Respond to election message from lower-ID node."""
    # Permite modificar la variable global 'in_election'.
    global in_election

    # Imprime un mensaje en consola indicando que se ha recibido un mensaje de elección desde el nodo emisor sender_id.
    print(f"[Node {NODE_ID}] Received ELECTION message from Node {sender_id}")

    # En el Algoritmo Bully, si el ID del emisor es menor que el ID del nodo actual (sender_id < NODE_ID):
    if sender_id < NODE_ID:
        # El nodo actual tiene mayor jerarquía ("bully"), por lo que debe tomar el control e iniciar su propia elección.
        if not in_election:
            # Inicia el proceso de elección del nodo actual de forma asíncrona (en un hilo secundario).
            start_election_async()

        # Responde con estatus 'ok' al nodo emisor inferior, notificándole que un nodo superior está activo y responderá.
        return {"status": "ok", "message": "OK", "node_id": NODE_ID}
    else:
        # Si el emisor tiene un ID mayor o igual, el nodo actual ignora el mensaje de elección ya que no puede desafiarlo.
        return {"status": "ignored", "message": "Sender ID is higher or equal", "node_id": NODE_ID}


def handle_victory_message(leader_node_id: int) -> Dict[str, Any]:
    """Handle victory message from new leader."""
    # Permite modificar las variables globales de estado de liderazgo.
    global leader_id, is_leader, in_election

    # Adquiere el cerrojo de sincronización para actualizar las variables globales de manera segura entre hilos.
    with _lock:
        # Asigna el ID del nodo victorioso como el nuevo líder reconocido del clúster.
        leader_id = leader_node_id
        # Establece 'is_leader' en True solo si el ID del líder coincide con el ID del nodo actual.
        is_leader = (leader_node_id == NODE_ID)
        # Marca que el proceso de elección ha finalizado.
        in_election = False

    # Muestra en consola el cambio de líder acordado.
    print(f"[Node {NODE_ID}] Node {leader_node_id} declared victory. New leader is Node {leader_id}.")

    # Retorna una confirmación en diccionario indicando que el mensaje de victoria fue procesado con éxito.
    return {"status": "ok", "leader_id": leader_id, "node_id": NODE_ID}


def declare_victory():
    """Announce self as leader to all nodes."""
    # Permite actualizar el estado global del nodo a líder.
    global is_leader, leader_id, in_election

    # Adquiere el cerrojo para actualizar el estado global sin interferencias de otros hilos.
    with _lock:
        # Se establece a sí mismo como líder activo.
        is_leader = True
        # Asigna su propio NODE_ID a leader_id.
        leader_id = NODE_ID
        # Finaliza el flag de proceso de elección.
        in_election = False

    # Imprime en consola la declaración de victoria.
    print(f"[Node {NODE_ID}] Declaring victory! I am the new leader.")

    # Obtiene la lista de todos los peers activos en la red.
    peers = get_peers()

    # Recorre cada nodo peer para notificarle sobre la victoria mediante una petición HTTP POST.
    for peer in peers:
        try:
            # Envía un objeto JSON con el tipo "victory" y el ID de este nodo como nuevo líder.
            requests.post(
                f"{peer}/election",
                json={"type": "victory", "leader_id": NODE_ID, "sender_id": NODE_ID},
                timeout=2.0
            )
        except Exception:
            # Si un peer está inalcanzable, se ignora el error y se continúa notificando a los demás.
            pass


def start_election():
    """Initiate an election, send ELECTION messages to higher-ID nodes."""
    # Accede a las variables globales para actualizar el estado de elección.
    global in_election, leader_id, is_leader

    # Adquiere el cerrojo para comprobar e iniciar la elección atómicamente.
    with _lock:
        # Si ya había una elección en curso, finaliza la ejecución para no duplicar elecciones concurrentes.
        if in_election:
            return
        # Marca in_election como True.
        in_election = True
        # Reinicia leader_id a None mientras transcurre la elección.
        leader_id = None
        # Marca is_leader como False provisionalmente.
        is_leader = False

    # Imprime en consola el inicio del proceso de elección.
    print(f"[Node {NODE_ID}] Starting leader election...")

    # Obtiene la lista de peers registrados.
    peers = get_peers()

    # Variable booleana local para rastrear si algún nodo con ID superior respondió afirmativamente al mensaje de elección.
    higher_nodes_responded = False

    # Recorre cada peer obtenido.
    for peer in peers:
        # Consulta o recupera el ID numérico del peer.
        peer_id = get_peer_id(peer)
        
        # En el algoritmo Bully, solo se envían mensajes de elección a nodos con ID mayor que el nodo actual.
        # Si el ID del peer es conocido y menor o igual que NODE_ID, se omite.
        if peer_id is not None and peer_id <= NODE_ID:
            continue

        try:
            # Envía petición HTTP POST con el mensaje de elección al peer superior.
            resp = requests.post(
                f"{peer}/election",
                json={"type": "election", "sender_id": NODE_ID},
                timeout=2.0
            )
            # Si la petición HTTP devuelve un estado 200 OK
            if resp.status_code == 200:
                data = resp.json()
                # Si el cuerpo de la respuesta indica 'ok', significa que un nodo de mayor ID está activo y asumirá la elección.
                if data.get("status") == "ok":
                    higher_nodes_responded = True
        except Exception:
            # Si el peer superior está caído o no responde antes del timeout, se ignora la excepción y se prueba con el siguiente.
            pass

    # Evaluación del resultado del envío a nodos superiores:
    if not higher_nodes_responded:
        # Si NINGÚN nodo con ID superior respondió (o no existen nodos superiores activos), este nodo gana la elección y se autodeclara líder.
        declare_victory()
    else:
        # Si al menos un nodo superior respondió OK, este nodo debe esperar la notificación de victoria de dicho nodo superior.
        # Se programa una comprobación con temporizador para evitar bloqueos en caso de que el nodo superior caiga durante la elección.
        def election_timeout_check():
            # Espera 5 segundos.
            time.sleep(5.0)
            global in_election, leader_id
            # Si tras 5 segundos aún no se ha establecido un líder (leader_id es None):
            if leader_id is None:
                with _lock:
                    # Libera el flag de elección y vuelve a intentar la elección.
                    in_election = False
                start_election_async()

        # Inicia el hilo temporizado en segundo plano (daemon thread).
        threading.Thread(target=election_timeout_check, daemon=True).start()


def start_election_async():
    """Helper to start election in a background thread."""
    # Crea y arranca un hilo secundario independiente para ejecutar start_election sin bloquear el hilo principal ni la respuesta HTTP.
    threading.Thread(target=start_election, daemon=True).start()


def find_leader_url(target_leader_id: int) -> Optional[str]:
    """Finds peer URL corresponding to target_leader_id."""
    # Obtiene la lista de peers conocidos.
    peers = get_peers()

    # Busca coincidencia exacta por ID recuperado del caché o consultas HTTP.
    for peer in peers:
        nid = get_peer_id(peer)
        if nid == target_leader_id:
            return peer

    # Intento heurístico por patrón de nombre de host en Docker (ej. node-3).
    for peer in peers:
        if f"node-{target_leader_id}" in peer:
            return peer

    # Si no se encuentra en la lista de peers, devuelve la URL formateada por defecto dentro de la red interna de Docker.
    return f"http://node-{target_leader_id}:8080"


def heartbeat_check():
    """Periodically check if leader is alive."""
    # Accede a las variables globales de liderazgo.
    global leader_id, is_leader, in_election

    # Si el nodo actual es el líder, no necesita comprobarse a sí mismo; finaliza la ejecución de la función.
    if is_leader:
        return

    # Si no hay ningún líder conocido en este momento (leader_id es None):
    if leader_id is None:
        # Si no hay una elección en curso, inicia una elección asíncrona.
        if not in_election:
            start_election_async()
        return

    # Obtiene la URL correspondiente al líder actual registrado.
    leader_url = find_leader_url(leader_id)
    # Si no es posible construir la URL del líder, invalida el líder y desencadena una nueva elección.
    if not leader_url:
        print(f"[Node {NODE_ID}] Could not determine leader URL for Node {leader_id}. Triggering election.")
        leader_id = None
        if not in_election:
            start_election_async()
        return

    try:
        # Realiza una petición GET al endpoint /health del nodo líder con un timeout estricto de 2.0 segundos.
        resp = requests.get(f"{leader_url}/health", timeout=2.0)
        # Si la respuesta no es un 200 OK, lanza una excepción para señalar que el líder ha fallado.
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code}")
    except Exception:
        # Si la petición HTTP falla o da timeout, el líder se considera caído o inaccesible.
        print(f"[Node {NODE_ID}] Leader {leader_id} at {leader_url} is unreachable. Triggering election...")
        # Resetea el ID del líder.
        leader_id = None
        # Si no se está ejecutando una elección activa, inicia una nueva elección para reemplazar al líder caído.
        if not in_election:
            start_election_async()


def heartbeat_loop():
    """Background loop that continuously runs heartbeat checks at set intervals."""
    # Espera inicial de 3.0 segundos al arrancar la aplicación para permitir la inicialización de la red y contenedores.
    time.sleep(3.0)

    # Si al finalizar el tiempo inicial no hay líder conocido ni elección activa, inicia una elección.
    if leader_id is None and not in_election:
        start_election_async()

    # Bucle infinito de monitoreo de latido (heartbeat)
    while True:
        try:
            # Ejecuta la comprobación de estado del líder.
            heartbeat_check()
        except Exception as e:
            # Captura y muestra cualquier excepción imprevista durante la comprobación.
            print(f"[Node {NODE_ID}] Heartbeat error: {e}")
        
        # Pausa la ejecución del bucle durante 4.0 segundos antes de realizar la siguiente comprobación.
        time.sleep(4.0)


def start_heartbeat_loop():
    """Starts the heartbeat monitoring loop in a background daemon thread."""
    # Crea un hilo de ejecución secundario (daemon=True para finalizar al cerrar la aplicación) que ejecuta heartbeat_loop.
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    # Arranca el hilo.
    t.start()


def get_election_state() -> Dict[str, Any]:
    """Returns current election state for node."""
    # Devuelve un diccionario estructurado con la foto del estado actual del algoritmo de elección en el nodo.
    # ¿Por qué está aquí?: Utilizado por los endpoints HTTP (GET /election, GET /health) para exponer el estado actual.
    return {
        "node_id": NODE_ID,
        "leader_id": leader_id,
        "is_leader": is_leader,
        "in_election": in_election,
        "peers": get_peers()
    }
