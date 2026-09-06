import socket

def recv_all(socket: socket.socket, size) -> bytes:
    chunks = []
    received = 0

    while(received < size):
        chunk = socket.recv(size - received)

        if chunk == b"":
            raise ConnectionError("socket closed before receiving all data")

        chunks.append(chunk)
        received+=len(chunk)

    return b"".join(chunks)
    #return socket.recv(size)


def send_all(socket: socket.socket, bytes) -> None:
    total_sent = 0

    while total_sent < len(bytes):
        sent = socket.send(bytes[total_sent:])

        #if sent == 0:
        #    raise ConnectionError("socket closed before sending all data")
        
        total_sent += sent


    #return socket.send(bytes)
