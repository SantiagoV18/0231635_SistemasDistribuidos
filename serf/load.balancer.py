import random
from flask import Flask, request
from serfclient import SerfClient

class LoadBalancer:
    def __init__(self, serf_client):
        self.serf_client = serf_client
        self.backends = []

    def update_backends(self):
        members = self.serf_client.members()
        self.backends = [
            member['tags']['backend_addr']
            for member in members if member['status'] == 'alive'
        ]
        print(f"Updated backends: {self.backends}")

    def get_backend(self):
        if not self.backends:
            return None
        return random.choice(self.backends)

app = Flask(__name__)
serf_client = SerfClient()
load_balancer = LoadBalancer(serf_client)

@app.route('/request', methods=['POST'])
def handle_request():
    load_balancer.update_backends()
    backend = load_balancer.get_backend()
    if not backend:
        return "No backends available", 503
    response = requests.post(f"http://{backend}/process", json=request.json)
    return response.content, response.status_code

if __name__ == "__main__":
    print("Starting load balancer...")
    load_balancer.update_backends()
    app.run(host="0.0.0.0", port=5000)
