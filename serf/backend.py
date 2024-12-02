import time
from flask import Flask, request
from serfclient import SerfClient

class Backend:
    def __init__(self, node_name, bind_addr):
        self.node_name = node_name
        self.bind_addr = bind_addr
        self.serf_client = SerfClient()

    def start(self):
        self.serf_client.agent("start", bind=self.bind_addr)
        self.serf_client.tags({
            "backend_addr": f"{self.bind_addr.split(':')[0]}:8000"
        })
        print(f"Backend {self.node_name} started at {self.bind_addr}.")

    def stop(self):
        self.serf_client.leave()
        print(f"Backend {self.node_name} left the cluster.")

app = Flask(__name__)
backend = Backend("backend1", "127.0.0.1:7946")

@app.route('/process', methods=['POST'])
def process_request():
    data = request.json
    time.sleep(1)  # Simula procesamiento
    return f"Processed: {data['message']}"

if __name__ == "__main__":
    backend.start()
    try:
        app.run(host="0.0.0.0", port=8000)
    except KeyboardInterrupt:
        backend.stop()
