
import threading
import grpc
import raft_pb2
import raft_pb2_grpc
import random
import time
from concurrent import futures
import sys
import os

random_time = 0

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, own_address, peer_addresses):
        self.node_id = node_id
        self.own_address = own_address  
        self.peer_addresses = peer_addresses
        self.current_term = -1
        self.voted_for = None
        self.state = 'follower'
        self.votes_received = set()
        self.rr = random_time
        self.election_timeout = time.time() + self.random_election_timeout()
        self.lease_time = 2
        self.lease_time_end = 0
        self.old_leader_lease_time_end = 0
        self.log = []
        self.leaderId = None
        file = open(f"log_node_{node_id}/log.txt","r")
        lod_data = file.read().split("\n")
        for data in lod_data:
            if data == "":
                continue
            data = data.split(";")
            self.log.append(raft_pb2.LogEntry(term=int(data[1]), command=data[0])) 
        file.close()
        self.database = {}
        self.next_index = {peer_address: len(self.log) + 1 for peer_address in self.peer_addresses}
        self.match_index = {peer_address: 0 for peer_address in self.peer_addresses}
        self.clientFlag = False
        self.Lock = threading.Lock()
        self.voteResponse = {
            peer_address: False for peer_address in self.peer_addresses
        }
        self.voteResponse[self.own_address] = True
        self.commit_index = len(self.log)-1
        print(f"Node {self.node_id} initialized as a follower with election timeout reset.")
        self.read_metadata()
        for entry in self.log:
            if entry.command != "NO-OP":
                command, key, value = entry.command.split()
                if command == "set":
                    self.database[key] = value
        self.write_metadata()
    def read_metadata(self):
        directory = f'log_node_{self.node_id}'
        if not os.path.exists(directory):
            os.makedirs(directory)
            return
        with open(os.path.join(directory, 'metadata.txt'), 'r') as f:
            lines = f.readlines()
            for line in lines:
                key, value = line.split(': ')
                if key == 'commitLength':
                    self.commit_index = int(value)
                elif key == 'Term':
                    self.current_term = int(value)
                elif key == 'VotedFor':
                    self.voted_for = int(value)

    def write_metadata(self):
        directory = f'log_node_{self.node_id}'
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(os.path.join(directory, 'metadata.txt'), 'w') as f:
            f.write(f'commitLength: {self.commit_index}\n')
            f.write(f'Term: {self.current_term}\n')
            f.write(f'VotedFor: {self.voted_for}\n')

    def append_to_dump(self, message):
        self.write_metadata()
        directory = f'log_node_{self.node_id}'
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(os.path.join(directory, 'dump.txt'), 'a') as f:
            f.write(message + "\n")


    def random_election_timeout(self):
        return self.rr

    def reset_election_timeout(self):
        self.election_timeout = time.time() + self.random_election_timeout()
        self.append_to_dump(f"Node {self.node_id} election timer reset")
        print(f'Term: {self.current_term}')
        print(f"Node {self.node_id} election timeout reset.")

    def RequestVote(self, request, context):
        print(f"Node {self.node_id} received vote request from Node {request.candidateId} for term {request.term}.")
        response = False
        self.reset_election_timeout()
        if request.term > self.current_term:
            self.append_to_dump(f"{self.node_id} Stepping down")
            self.current_term = request.term
            self.voted_for = None
            self.state = 'follower'
            self.write_metadata()

        if (self.voted_for is None or self.voted_for == request.candidateId) and request.term == self.current_term:
            self.voted_for = request.candidateId
            self.append_to_dump(f"Vote granted for Node {request.candidateId} in term {request.term}.")  # Logging statement
            response = True
            self.write_metadata()
            print(f"Node {self.node_id} granted vote to Node {request.candidateId} for term {request.term}.")
        else:
            self.append_to_dump(f"Vote denied for Node {request.candidateId} in term {request.term}.")  # Logging statement
            print(f"Node {self.node_id} denied vote to Node {request.candidateId} for term {request.term}.")
        self.write_metadata()
        return raft_pb2.VoteResponse(term=self.current_term, voteGranted=response)

  

    def Heartbeat(self, request, context):
        print("Current_term",self.current_term)
        print(f"Node {self.node_id} received heartbeat for term {request.term}.")
        
        if request.term < self.current_term:
            self.append_to_dump(f"Node {self.node_id} rejected AppendEntries RPC from {request.leaderId}.")  # Logging statement
            return raft_pb2.HeartbeatResponse(term=self.current_term, success=False)
        self.leaderId = request.leaderId
          # Logging statement
        self.old_leader_lease_time_end = request.leaseDuration
        self.state = 'follower'
        self.current_term = request.term
        self.reset_election_timeout()
        if request.prevLogIndex > len(self.log) or (request.prevLogIndex > 0 and self.log[request.prevLogIndex - 1].term != request.prevLogTerm):
            self.append_to_dump(f"Node {self.node_id} rejected AppendEntries RPC from {request.leaderId}.")
            return raft_pb2.HeartbeatResponse(term=self.current_term, success=False)
        self.append_to_dump(f"Node {self.node_id} accepted AppendEntries RPC from {request.leaderId}.")
        if request.entries:
            new_entries_start_index = request.prevLogIndex
            self.log = self.log[:new_entries_start_index]
            for entry in request.entries:
                if new_entries_start_index < len(self.log):
                    self.log[new_entries_start_index] = entry
                    
                else:
                    self.log.append(entry)
                new_entries_start_index += 1
                
            with open(f"log_node_{self.node_id}/log.txt", "w") as file:
                for entry in self.log:
                    file.write(f"{entry.command};{entry.term}\n")

        if request.leaderCommit > self.commit_index:
            for i in range(self.commit_index + 1, min(request.leaderCommit, len(self.log)) + 1):
                entry = self.log[i - 1]
                if entry.command != "NO-OP":
                    command, key, value = entry.command.split()
                    if command == "set":
                        self.database[key] = value
                        self.append_to_dump(f"Applied SET {key} = {value} to the state machine.")
            # self.commit_index = min(request.leaderCommit, len(self.log))
            self.commit_index = request.leaderCommit
        self.write_metadata()
        return raft_pb2.HeartbeatResponse(term=self.current_term, success=True)

    def request_vote(self, peer_address):
        with grpc.insecure_channel(peer_address) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            try:
                response = stub.RequestVote(raft_pb2.VoteRequest(term=self.current_term, candidateId=self.node_id))
                if response.voteGranted:
                    self.votes_received.add(peer_address)
                    print(f"Node {self.node_id} received a vote from {peer_address} for term {self.current_term}.")
                    
            except grpc.RpcError:
                print(f"Node {self.node_id} failed to request vote from {peer_address}")
        self.write_metadata()

    def start_election(self):
        self.state = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        print(f"Node {self.node_id} starting election for term {self.current_term}.")

        for peer_address in self.peer_addresses:
            self.request_vote(peer_address)
        if len(self.votes_received) > (len(self.peer_addresses) + 1) / 2 and self.state == 'candidate':
            self.become_leader()
        self.write_metadata()
        
    # def start_election(self):
    #     self.state = 'candidate'
    #     self.current_term += 1
    #     self.voted_for = self.node_id
    #     self.votes_received = {self.node_id}
    #     self.reset_election_timeout()
    #     print(f"Node {self.node_id} starting election for term {self.current_term}.")
    #     # Thread_map = {
    #     #     peer_address: threading.Thread(target=self.request_vote_thread,args=(raft_pb2_grpc.RaftStub(grpc.insecure_channel(peer_address)),peer_address)) for peer_address in self.peer_addresses
    #     # }
    #     # for peer_address in self.peer_addresses:
    #     #     self.request_vote(peer_address,Thread_map[peer_address])
            
    #     # for peer_address in self.peer_addresses:
    #     #     Thread_map[peer_address].join()  


    #     if len(self.votes_received) >= (len(self.peer_addresses)//2 + 1) and self.state == 'candidate':
    #         self.become_leader()
    #     self.write_metadata()


    # def request_vote_thread(self,stub,peer_address):
    #     response = stub.RequestVote(raft_pb2.VoteRequest(term=self.current_term, candidateId=self.node_id))
    #     if response.voteGranted:
    #         self.voteResponse[peer_address] = True
    #         print(f"Node {self.node_id} received a vote from {peer_address} for term {self.current_term}.")
    #         # if len(self.votes_received) > (len(self.peer_addresses) + 1) / 2 and self.state == 'candidate':
    #         #     self.become_leader()
    #     else:
    #         print(f"Node {self.node_id} failed to request vote from {peer_address}")
    #         self.votes_received[peer_address] = False
    #     return
    # def request_vote(self, peer_address, Thread_id):
    #     with self.Lock:
    #         with grpc.insecure_channel(peer_address) as channel:
    #             stub = raft_pb2_grpc.RaftStub(channel)
    #             Thread_id.start()
    #             if self.voteResponse[peer_address] == True:
    #                 self.votes_received.add(peer_address)
    #                 self.voteResponse[peer_address] = False
    #             # try:
    #             #     if len(self.votes_received) > (len(self.peer_addresses) + 1) / 2 and self.state == 'candidate':
    #             #         self.become_leader()
    #             # except grpc.RpcError:
    #             #     print(f"Node {self.node_id} failed to request vote from {peer_address}")
        

    def become_leader(self):
        self.state = 'leader'
        self.leaderId = self.node_id
        self.append_to_dump(f"Node {self.node_id} became the leader for term {self.current_term}.")
        self.write_metadata()
        self.send_heartbeats(1)
        

    def send_heartbeats(self,flag):
        while self.state == 'leader':
            if time.time()<self.old_leader_lease_time_end:
                self.append_to_dump("New Leader waiting for Old Leader Lease to timeout")
                print("Cannot send heartbeat as old lease time not expired")
                continue
            self.append_to_dump(f"Leader {self.node_id} sending heartbeat & Renewing Lease")  # Logging statement
            if flag == 1:
                self.log.append(raft_pb2.LogEntry(term=self.current_term, command="NO-OP"))
                flag = 0
            if(self.lease_time_end == 0):
                self.lease_time_end = time.time() + self.lease_time
            count = 1
            file = open(f"log_node_{node_id}/log.txt","w")
            for entry in self.log:
                file.write(entry.command)
                file.write(";")
                file.write(str(entry.term))
                file.write("\n")
            file.close()
            print(f"Node {self.node_id} sending heartbeats.")


            for peer_address in self.peer_addresses:
                with grpc.insecure_channel(peer_address) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    next_index_peer = self.next_index[peer_address]
                    prevLogIndex = next_index_peer - 1
                    prevLogTerm = self.log[prevLogIndex - 1].term if prevLogIndex > 0 else 0
                    entries_to_send = self.log[prevLogIndex:]

                    try:
                        response = stub.Heartbeat(raft_pb2.HeartbeatRequest(
                            term=self.current_term,
                            leaseDuration=self.lease_time_end,
                            entries=entries_to_send,
                            leaderCommit=self.commit_index,
                            prevLogIndex=prevLogIndex,
                            prevLogTerm=prevLogTerm,
                            leaderId=self.node_id)
                        )
                        if response.success:
                            self.match_index[peer_address] = prevLogIndex + len(entries_to_send)
                            self.next_index[peer_address] = self.match_index[peer_address] + 1
                            count += 1
                        else:
                            self.next_index[peer_address] = max(1, self.next_index[peer_address] - 1)
                    except grpc.RpcError:
                        self.append_to_dump(f"Error occurred while sending RPC to Node {peer_address}.")  # Logging statement
                        print(f"Failed to send heartbeat to {peer_address}")
            
            if count > (len(self.peer_addresses)) / 2:
                print(count)
                # print("commtting log")
                try:
                    for entry in self.log:
                        if(entry.command == "NO-OP"):
                            continue
                        data = entry.command.split()
                        self.database[data[1]] = data[2]
                        self.append_to_dump(f"Applied SET {data[1]} = {data[2]} to the state machine.")
                    print("Database:",self.database)
                    self.commit_index = len(self.log)
                    self.lease_time_end = self.lease_time_end + self.lease_time

                except:
                    print("Error in committing log",sys.exc_info())
                self.clientFlag = True
                #time.sleep(0.5)
            elif time.time() > self.lease_time_end:
                print(f"Node {self.node_id} lease time expired. Becoming follower and resetting election timeout.")
                self.state = 'follower'
                self.reset_election_timeout()
                self.append_to_dump(f"Leader {self.node_id} stepping down, term outdated.")
            else:
                print("jhinga laala ho ho")
            self.write_metadata()
            time.sleep(1)
                
    def monitor_state(self):
        while True:
            if self.state != 'leader' and time.time() > self.election_timeout:
                self.append_to_dump(f"Node {self.node_id} election timer timed out, Starting election.")  # Logging statement
                print(f"Node {self.node_id} election timeout. Starting election.")
                self.start_election()
            # time.sleep(0.5)
    
    def ClientRequest(self, request, context):
        if self.state == 'leader':
            self.append_to_dump(f"Node {self.node_id} (leader) received an {request.request} request.")  # Logging statement
            print("Old lease time end",self.old_leader_lease_time_end)
            print("Current time",time.time())
            if time.time() < self.old_leader_lease_time_end:
                self.append_to_dump("New Leader waiting for Old Leader Lease to timeout.")
                if request.request.split()[0] == "SET":
                    print(f"Node {self.node_id} received client request but currently old lease time not expired.")
                    return raft_pb2.ClientResponseMessage(success = False, message="Leader in old lease time" ,leaderAddress=self.node_id)
                elif request.request.split()[0] == "GET":
                    m = self.database.get(request.request.split()[1])
                    if m == None:
                        return raft_pb2.ClientResponseMessage(success = True, message="Not a leader",leaderAddress=self.node_id)
                    else:
                        return raft_pb2.ClientResponseMessage(success = True, message=m,leaderAddress=self.node_id)
                else:
                    return raft_pb2.ClientResponseMessage(success = False, message="Not a leader",leaderAddress=self.voted_for)
            else:
                print(f"Node {self.node_id} received client request.")
                if request.request.split()[0] == "SET":
                    self.log.append(raft_pb2.LogEntry(term=self.current_term, command=request.request))
                elif request.request.split()[0] == "GET":
                    m = self.database.get(request.request.split()[1])
                    if m == None:
                        return raft_pb2.ClientResponseMessage(success = False, message="",leaderAddress=self.node_id)
                    else:
                        return raft_pb2.ClientResponseMessage(success = True, message=m,leaderAddress=self.node_id)
                while(self.clientFlag == False):
                    pass
                self.clientFlag = False
                return raft_pb2.ClientResponseMessage(success = True, message="Leader in new lease time" ,leaderAddress=self.node_id)
        else:
            print(f"Node {self.node_id} received client request.")
            return raft_pb2.ClientResponseMessage(success = False,message = "Not a leader",leaderAddress=self.voted_for)


    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
        raft_pb2_grpc.add_RaftServicer_to_server(self, server)
        server.add_insecure_port(self.own_address)
        server.start()
        print(f"Node {self.node_id} started on {self.own_address}.")
        self.monitor_state()
        server.wait_for_termination()

# if __name__ == "__main__":
#     l = sys.argv[1].split()
#     random_time = int(l[1])
#     random_time = random.randint(5,11)
#     node_id = int(l[0])
#     own_address = f"localhost:500" + str(node_id)
#     peer_addresses = [f"localhost:500{i}" for i in range(1, 6) if i != node_id]
#     node = RaftNode(node_id, own_address, peer_addresses)
#     node.serve()

if __name__ == "__main__":
    random_time = random.randint(5,11)
    node_id = 1
    ip = "34.131.76.11"
    own_address = f"{ip}:500" + str(node_id)
    peer_addresses = ["34.131.101.219:5001","34.131.38.160:5002","34.131.76.11:5003","34.131.103.231:5004","34.131.214.36:5005"]
    peer_addresses.remove(own_address)

    node = RaftNode(node_id, "0.0.0.0:500"+ str(node_id), peer_addresses)
    node.serve()