import grpc
import raft_pb2
import raft_pb2_grpc
import random
import time
import sys

class RaftClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.leader_address = None
    def send_request(self, request):
        node_address = random.choice(self.node_addresses)
        while True:
            with grpc.insecure_channel(node_address) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                try:
                    # Corrected the way ClientRequestMessage is constructed and passed
                    client_request_message = raft_pb2.ClientRequestMessage(request=request)
                    
                    response = stub.ClientRequest(client_request_message)
                    print(response.message)
                    if response.success:
                        print("Request processed successfully.")
                        self.leader_address = response.leaderAddress
                        break
                    else:
                        # Assuming leaderAddress is a string containing the address, not an index
                        if response.message == "Not a leader":
                            # node_address = random.choice(self.node_addresses)
                            node_address = self.node_addresses[response.leaderAddress-1]
                            
                            print(f"Redirect to leader at {response.leaderAddress} or wait for lease expiration.")
                except grpc.RpcError as e:
                    print(f"Failed to send request {node_address}:", e)
                    node_address = random.choice(self.node_addresses)
            time.sleep(2)

if __name__ == "__main__":
    nodes = ["34.131.101.219:5001","34.131.38.160:5002","34.131.76.11:5003","34.131.103.231:5004","34.131.214.36:5005"]
    client = RaftClient(nodes)
    while True:
        input_help = str(input("Enter the request: "))
        if(input_help == "exit"):
            break
        client.send_request(input_help)
