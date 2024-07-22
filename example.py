import socket
import ssl
import asyncio
import zmq
import zmq.asyncio
import signal
import struct
import threading


class TCPProxy:
    def __init__(self, host, port, proxy_port) -> None:
        self.host = host
        self.port = port
        self.proxy_host = "127.0.0.1"
        self.proxy_port = proxy_port
        self.running = True

    def setup_proxy(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.proxy_host, self.proxy_port))
            server_socket.listen(5)
            print(f"Listening on {self.proxy_host}:{self.proxy_port}")

            while self.running:
                try:
                    client_socket, _ = server_socket.accept()
                    print("Accepted connection from client")
                    client_handler = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, self.host, self.port)
                    )
                    client_handler.start()
                except Exception as e:
                    print(f"Error accepting connection: {e}")

    def handle_client(self, client_socket, target_host, target_port):
        try:
            # Connect to the target server
            target_socket = create_tls_tcp_socket(target_host, target_port)

            # Forward data between client and target server
            def forward(source, destination):
                while self.running:
                    data = source.recv(4096)
                    if not data:
                        break
                    destination.sendall(data)

            # Create threads to handle bidirectional forwarding
            client_thread = threading.Thread(target=forward, args=(client_socket, target_socket))
            target_thread = threading.Thread(target=forward, args=(target_socket, client_socket))
            client_thread.start()
            target_thread.start()
            client_thread.join()
            target_thread.join()
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def stop(self):
        self.running = False


def create_tls_tcp_socket(hostname, port):
    try:
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ssl_context.maximum_version = ssl.TLSVersion.TLSv1_2
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        tcp_sock = socket.create_connection((hostname, port))
        tls_sock = ssl_context.wrap_socket(tcp_sock, do_handshake_on_connect=True, server_hostname=hostname)
        print(f"Connected with TLS: {tls_sock.version()}")
        return tls_sock
    except Exception as e:
        print(f"Error creating TLS socket: {e}")
        return None


class ZMQHandler:
    def __init__(self, port):
        self.loop = asyncio.get_event_loop()
        self.zmqContext = zmq.asyncio.Context()
        self.running = True

        self.zmqSubSocket = self.zmqContext.socket(zmq.SUB)
        self.zmqSubSocket.setsockopt(zmq.RCVHWM, 0)
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashtx")
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "rawblock")
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "rawtx")
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "sequence")
        print(f"Connecting to: tcp://127.0.0.1:{port}")
        self.zmqSubSocket.connect(f"tcp://127.0.0.1:{port}")

    async def handle(self):
        while self.running:
            try:
                topic, body, seq = await self.zmqSubSocket.recv_multipart()
                sequence = "Unknown"
                if len(seq) == 4:
                    sequence = str(struct.unpack('<I', seq)[-1])
                if topic == b"hashblock":
                    print('- HASH BLOCK (' + sequence + ') -')
                    print(body.hex())
                elif topic == b"hashtx":
                    print('- HASH TX  (' + sequence + ') -')
                    print(body.hex())
                elif topic == b"rawblock":
                    print('- RAW BLOCK HEADER (' + sequence + ') -')
                    print(body[:80].hex())
                elif topic == b"rawtx":
                    print('- RAW TX (' + sequence + ') -')
                    print(body.hex())
                elif topic == b"sequence":
                    hash = body[:32].hex()
                    label = chr(body[32])
                    mempool_sequence = None if len(body) != 32 + 1 + 8 else struct.unpack("<Q", body[32 + 1:])[0]
                    print('- SEQUENCE (' + sequence + ') -')
                    print(hash, label, mempool_sequence)
            except Exception as e:
                print(f"Error receiving message: {e}")

    def start(self):
        self.loop.create_task(self.handle())
        self.loop.run_forever()

    def stop(self):
        self.running = False
        self.zmqContext.destroy()
        self.loop.stop()


if __name__ == "__main__":
    hostname = "" #remote host name, ex. 'bcntplorcd.b.voltageapp.io'
    port = 28332 #default zmq port for bitcoind
    proxy_port = port - 1

    tcp_proxy = TCPProxy(hostname, port, proxy_port)
    proxy_thread = threading.Thread(target=tcp_proxy.setup_proxy)
    proxy_thread.start()

    zmq_handler = ZMQHandler(proxy_port)

    try:
        zmq_handler.start()
    except KeyboardInterrupt:
        print("Shutting down...")
        zmq_handler.stop()
        tcp_proxy.stop()
        proxy_thread.join()
        print("Shutdown complete.")
