import os
import socket
import threading
import logger
import safe_socket
import protocol
from lottery.lottery import Lottery

_STORAGE_PATH = "/tmp/bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(_STORAGE_PATH)


    def _handle_client(self, client_socket):
        action = "handle-client"
        agency_id = None
        message_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                msg_type, payload = protocol.read_message(safe_socket, client_socket)
                if msg_type == protocol.MSG_BET_BATCH:
                    try:
                        agency_id_str, bets_payload = payload.split(b"|", 1)
                        agency_id = int(agency_id_str)
                        bets = protocol.decode_bets(agency_id, bets_payload)
                        self.lottery.store_bets(bets)
                        protocol.write_message(safe_socket, client_socket, protocol.MSG_BATCH_ACK, b"\x00")
                    except Exception:
                        protocol.write_message(safe_socket, client_socket, protocol.MSG_BATCH_ACK, b"\x01")
                elif msg_type == protocol.MSG_DONE:
                    winners = [
                        bet.document for bet in self.lottery.load_bets()
                        if bet.agency_id == agency_id and self.lottery.has_won(bet)
                    ]

                    protocol.write_message(
                        safe_socket, client_socket, protocol.MSG_WINNERS,
                        protocol.encode_winners(winners),
                    )
                    logger.info(action, logger.LogResult.success)
                    return
        except Exception as e:
            logger.error(action, logger.LogResult.fail)
            raise e

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
