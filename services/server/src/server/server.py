import os
import socket
import threading
import logger
import safe_socket
import protocol
from lottery.lottery import Lottery

_STORAGE_PATH = "/tmp/bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.lottery = Lottery(_STORAGE_PATH)

        self.store_lock = threading.Lock()
        self.quorum_lock = threading.Lock()
        self.quorum_cond = threading.Condition(self.quorum_lock)
        self.finished_agencies = set()

    def _wait_for_quorum(self, agency_id: int):
        with self.quorum_cond:
            self.finished_agencies.add(agency_id)
            while len(self.finished_agencies) < self.agency_quorum_min:
                self.quorum_cond.wait()
            self.quorum_cond.notify_all()

    def _handle_client(self, client_socket):
        action = "handle-client"
        agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                msg_type, payload = protocol.read_message(safe_socket, client_socket)
                if msg_type == protocol.MSG_BET_BATCH:
                    try:
                        agency_id_str, bets_payload = payload.split(b"|", 1)
                        agency_id = int(agency_id_str)
                        bets = protocol.decode_bets(agency_id, bets_payload)
                        with self.store_lock:
                            self.lottery.store_bets(bets)
                        protocol.write_message(safe_socket, client_socket, protocol.MSG_BATCH_ACK, b"\x00")
                    except Exception:
                        protocol.write_message(safe_socket, client_socket, protocol.MSG_BATCH_ACK, b"\x01")
                elif msg_type == protocol.MSG_DONE:
                    self._wait_for_quorum(agency_id)
                    with self.store_lock:
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
            client_socket.close()
            #raise e

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                    logger.info(action, logger.LogResult.success)
                    threading.Thread(
                        target=self._handle_client, args=(client_socket,), daemon=True
                    ).start()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                #self._handle_client(client_socket)