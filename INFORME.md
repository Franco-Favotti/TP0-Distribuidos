# TP Nivelador: Docker, Comunicaciones y Concurrencia

## 1. Introducción

Este informe explica los aspectos más importantes de la solución provista: el protocolo de comunicación implementado entre cliente y servidor, los mecanismos utilizados para sincronizar la ejecución concurrente del servidor, y las librerías empleadas en cada lenguaje.

## 2. Protocolo de comunicación

### 2.1 Framing de mensajes

Se diseñó un framing binario simple, sin ninguna librería de serialización automática. 
Cada mensaje tiene:

| Campo | Tamaño | Descripción |
|---|---|---|
| Tipo | 1 byte | Tipo de mensaje |
| Longitud | 4 bytes (big endian) | Tamaño del payload |
| Payload | variable | Contenido de cada mensaje |

Del lado servidor, el header se arma y se parsea a mano con los métodos nativos `int.to_bytes()` / `int.from_bytes()`. Del lado cliente, se usa `encoding/binary` solo para las funciones de orden de bytes (`binary.BigEndian.PutUint32`/`Uint32`).

### 2.2 Tipos de mensaje

| Valor | Nombre | Dirección | Payload |
|---|---|---|---|
| 1 | `BET_BATCH` | cliente → servidor | agency_id + una o más apuestas serializadas como texto |
| 2 | `BATCH_ACK` | servidor → cliente | 1 byte: `0x00` éxito, `0x01` error |
| 3 | `DONE` | cliente → servidor | vacío — indica fin de transmisión de esa agencia |
| 4 | `WINNERS` | servidor → cliente | lista de documentos ganadores de esa agencia |

### 2.3 Serialización de las apuestas

Cada apuesta se serializa como texto plano separado por comas (`first_name,last_name,document,birthdate,number`), con un salto de línea entre apuestas dentro de un mismo batch.

### 2.4 Separación dominio / comunicación y diseño de memoria acotada

Para evitar retener en memoria todo el contenido del `INPUT_FILE` (lo cual escalaría linealmente con el tamaño del archivo), el cliente procesa el archivo en dos pasadas:

1. **Primera pasada**: lee el archivo línea por línea, agrupa las apuestas en lotes de tamaño `BATCH_SIZE` y los envía. Solo se mantiene en memoria el lote en curso; nunca el archivo completo.
2. **Segunda pasada**: Tras recibir `WINNERS`se guarda únicamente el conjunto de documentos ganadores el cual denota un tamaño relativamente chico, se relee el archivo desde el principio con el mismo descriptor, y se copian a `OUTPUT_FILE` únicamente las líneas cuyo documento está en ese conjunto.

Este diseño evita que el pico de memoria del proceso crezca proporcionalmente al tamaño del archivo de entrada. Únicamente implica mas tiempo que a fines generales sigue siendo O(n) por lo tanto el costo es bajo.

## 3. Batching

El cliente agrupa hasta `BATCH_SIZE` apuestas por mensaje antes de enviarlas, reduciendo la cantidad de mensajes de red. El servidor decodifica todas las apuestas de un batch y las almacena como una unidad atómica: si una apuesta del batch es inválida, se descarta el batch completo y se responde con `BATCH_ACK` de error, sin persistir apuestas parciales. El cliente no arma ni envía el próximo batch hasta recibir la confirmación del anterior

## 4. Concurrencia y sincronización

### 4.1 Herramienta de concurrencia elegida: threads

Se optó por threading (uno por cada conexión de cliente aceptada) en lugar de multiprocessing. Se tomó esta decisión ya que el trabajo del servidor se basa predominantemente en esperar operaciones de entrada/salida, no CPU-bound: durante las esperas de E/S, el GIL de Python se libera, por lo que múltiples threads pueden progresar de forma efectivamente concurrente sin que el GIL se convierta en un cuello de botella. 

### 4.2 Mutex — exclusión mutua sobre el storage compartido

self.store_lock = threading.Lock()

Protege el acceso concurrente al storage de `Lottery`. Es un mutex simple: garantiza que un único thread a la vez lea o escriba el archivo compartido entre todas las agencias.

### 4.3 Monitor — coordinación del quorum entre agencias

self.quorum_cond = threading.Condition(self.quorum_lock)

Para esperar a que se junte un mínimo de agencias (`AGENCY_QUORUM_MIN`) antes de calcular el sorteo, se usa un monitor, no solo un mutex: cada thread que termina de recibir las apuestas de su agencia se registra como "agencia terminada" y queda bloqueado (wait()) hasta que se cumpla la condición de quorum, momento en el cual se despierta a todos los threads en espera (notify_all()).

### 4.4 Filtrado de ganadores por agencia

Una vez liberado el quorum, cada thread calcula de forma independiente solo los ganadores correspondientes a su propia agencia. El sorteo es único y global (mismo número ganador para todas las agencias), pero ninguna agencia recibe información sobre ganadores de otra agencia.

## 5. Manejo de SIGTERM

### 5.1 Servidor

- Un `threading.Event` (`self._shutdown`) señaliza el pedido de cierre.
- `shutdown()` marca el evento, notifica (`notify_all()`) a los threads bloqueados esperando el quorum, y cierra el socket de escucha (`self._server_socket.close()`) para desbloquear inmediatamente el `accept()` del loop principal, que estaba retenido esperando nuevas conexiones.
- El `OSError` resultante de cerrar el socket en medio de un `accept()` bloqueado se captura explícitamente (`except OSError: break`) para terminar el loop de forma ordenada en vez de propagar una excepción no controlada.

### 5.2 Cliente

- Un flag atómico (`atomic.Bool`) indica si se solicitó el cierre.
- `Shutdown()` marca el flag y cierra la conexión (`conn.Close()`), lo cual desbloquea cualquier `Read`/`Write` pendiente en curso.
- Como cerrar la conexión de forma intencional genera un error de E/S en la operación bloqueada, el punto donde se decide el código de salida del proceso consulta explícitamente si el cierre fue voluntario (`IsShuttingDown()`) antes de reportar el error como una falla real. Esto asegura que una terminación ordenada por SIGTERM siempre resulte en código de salida `0`, distinguiéndola de un error de comunicación genuino.

## 6. Librerías utilizadas y justificación

### 6.1 Permitidas explícitamente y utilizadas

| Librería | Lenguaje | Uso |
|---|---|---|
| `signal` | Python | Captura de SIGTERM en el servidor | 
| `threading` | Python | Threads, `Lock`, `Condition`, `Event` | 
| `os/signal` | Go | Captura de SIGTERM en el cliente |
| `syscall` | Go | Constante `syscall.SIGTERM` para referenciar la señal| 
| `sync/atomic` | Go | Flag de shutdown (`atomic.Bool`) leído/escrito desde distintas goroutines | 
| `encoding/binary` | Go | Únicamente `binary.BigEndian.PutUint32`/`Uint32` para el header del protocolo | 
| `bufio` | Go | Lectura del `INPUT_FILE` línea por línea (`bufio.NewScanner`) | 
