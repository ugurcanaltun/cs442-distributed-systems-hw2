import multiprocessing
from channel import Channel

def server_program():
    channel_id = 1
    server_channel = Channel(channel_id, flush=True)

    # Join the channel for the server process
    server_channel.join(1) 

    for data in range(1, 101):
        # Send the data to the client
        print(f"Server: Sending data {data}")
        server_channel.sendToAll(data)

        # Wait for acknowledgment from the client
        sender, ack = server_channel.recvFromAny()
        print(f"Server: Received acknowledgment from client {sender}: {ack}")

def client_program():
    channel_id = 1
    client_channel = Channel(channel_id)

    # Join the channel for the client process
    client_channel.join(2)

    for _ in range(100):
        # Receive data from the server
        sender, data = client_channel.recvFromAny()
        print(f"Client: Received data from server {sender}: {data}")

        # Send acknowledgment to the server
        client_channel.sendTo(sender, "ack")


# Create separate processes for the server and client
server_process = multiprocessing.Process(target=server_program)
client_process = multiprocessing.Process(target=client_program)

# Start both processes
server_process.start()
client_process.start()

# Wait for both processes to finish
server_process.join()
client_process.join()