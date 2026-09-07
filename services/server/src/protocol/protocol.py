from lottery.bet import Bet

MSG_BET_BATCH = 1
MSG_BATCH_ACK = 2
MSG_DONE = 3
MSG_WINNERS = 4

_HEADER_SIZE = 5 # 1 byte para el tipo de mensaje + 4 bytes para el tamaño del payload

#Lee un mensaje, interpretando su Header y payload.
def read_message(safe_socket, socket):
    header = safe_socket.recv_all(socket, _HEADER_SIZE)
    msg_type = header[0]
    length = int.from_bytes(header[1:5], byteorder="big", signed=False)
    payload = safe_socket.recv_all(socket, length) if length > 0 else b""

    return msg_type, payload

#Construye y envia un mensaje siguiendo el formato del protocolo
def write_message(safe_socket, socket, msg_type: int, payload: bytes = b""):
    header = bytes([msg_type]) + len(payload).to_bytes(4, byteorder="big", signed=False)
    safe_socket.send_all(socket, header + payload)

#Convierte el payload de texto en una lista de objetos Bet
def decode_bets(agency_id: int, payload: bytes) -> list[Bet]:
    bets = []
    for line in payload.decode("utf-8").split("\n"):
        if not line:
            continue
        first_name, last_name, document, birthdate, number = line.split(",")
        bets.append(
            Bet(agency_id, first_name, last_name, int(document), birthdate, int(number))
        )
    return bets

#Convierte una lista de documentos ganadores en el payload de la respuesta
def encode_winners(documents: list[int]) -> bytes:
    return ",".join(str(doc) for doc in documents).encode("utf-8")