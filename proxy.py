#!/usr/bin/env python3
import sys
import socket
import select
import time
import xml.etree.ElementTree as ET
import logging
from collections import defaultdict

class VideoProxy:
    def __init__(self, log_file, alpha, port):
        self.alpha = float(alpha)
        self.port = int(port)
        self.server_ip = "149.165.170.233"  # Hardcoded DASH server IP
        self.server_port = 80
        
        # Setup logging
        logging.basicConfig(filename=log_file, level=logging.INFO, 
                          format='%(message)s')
        
        # Connection management
        self.epoll = select.epoll()
        self.connections = {}
        self.requests = {}
        self.responses = {}
        
        # Video streaming state
        self.available_bitrates = []
        self.current_throughput = defaultdict(float)  # Per client throughput
        self.chunk_start_times = {}
        self.manifest_cache = None
        
    def setup_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('', self.port))
        server_socket.listen(1)
        server_socket.setblocking(False)
        
        self.epoll.register(server_socket.fileno(), select.EPOLLIN)
        self.server_socket = server_socket
        
    def parse_manifest(self, manifest_data):
        """Extract available bitrates from manifest file"""
        root = ET.fromstring(manifest_data)
        bitrates = []
        for rep in root.findall(".//{*}Representation"):
            bitrate = int(rep.get('bandwidth')) // 1000  # Convert to Kbps
            bitrates.append(bitrate)
        return sorted(bitrates)
        
    def select_bitrate(self, client_id):
        """Select highest bitrate that current throughput can support"""
        current_tput = self.current_throughput[client_id]
        
        for bitrate in reversed(self.available_bitrates):
            if current_tput >= bitrate * 1.5:
                return bitrate
        
        return self.available_bitrates[0]  # Return lowest bitrate if none suitable
        
    def update_throughput(self, client_id, chunk_size, download_time):
        """Update EWMA throughput estimate"""
        if download_time > 0:
            measured_tput = (chunk_size * 8) / (download_time * 1000)  # Convert to Kbps
            if self.current_throughput[client_id] == 0:
                self.current_throughput[client_id] = measured_tput
            else:
                self.current_throughput[client_id] = (
                    self.alpha * measured_tput + 
                    (1 - self.alpha) * self.current_throughput[client_id]
                )
            return measured_tput
        return 0
        
    def handle_request(self, client_socket, request_data):
        """Process incoming HTTP request"""
        request_lines = request_data.decode('utf-8').split('\r\n')
        if not request_lines:
            return None
            
        # Parse request line
        method, path, _ = request_lines[0].split(' ')
        
        # Handle manifest file request
        if 'manifest.mpd' in path:
            if not self.manifest_cache:
                # Fetch actual manifest from server
                server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_sock.connect((self.server_ip, self.server_port))
                server_sock.send(request_data)
                manifest_data = server_sock.recv(65536)
                server_sock.close()
                
                # Parse and cache manifest
                self.manifest_cache = manifest_data
                self.available_bitrates = self.parse_manifest(manifest_data.decode('utf-8'))
            
            # Modify request to use manifest_nolist.mpd
            modified_request = request_data.decode('utf-8').replace(
                'manifest.mpd', 'manifest_nolist.mpd'
            ).encode('utf-8')
            return modified_request
            
        # Handle video chunk requests
        if 'Seg' in path:
            client_id = client_socket.fileno()
            chunk_name = path.split('/')[-1]
            
            # Record start time for throughput calculation
            self.chunk_start_times[client_id] = time.time()
            
            # Modify bitrate in request
            new_bitrate = self.select_bitrate(client_id)
            current_bitrate = int(chunk_name.split('Seg')[0])
            if current_bitrate != new_bitrate:
                modified_path = path.replace(
                    f"{current_bitrate}Seg",
                    f"{new_bitrate}Seg"
                )
                modified_request = request_data.decode('utf-8').replace(
                    path, modified_path
                ).encode('utf-8')
                return modified_request
                
        return request_data
        
    def handle_response(self, client_socket, response_data):
        """Process server response"""
        client_id = client_socket.fileno()
        
        # Calculate throughput for video chunks
        if client_id in self.chunk_start_times:
            download_time = time.time() - self.chunk_start_times[client_id]
            content_length = 0
            
            # Parse content length from headers
            headers = response_data.split(b'\r\n\r\n')[0].decode('utf-8')
            for line in headers.split('\r\n'):
                if 'Content-Length' in line:
                    content_length = int(line.split(': ')[1])
                    break
            
            if content_length > 0:
                measured_tput = self.update_throughput(
                    client_id, content_length, download_time
                )
                
                # Log chunk download information
                chunk_name = self.requests[client_id].decode('utf-8').split(' ')[1].split('/')[-1]
                if 'Seg' in chunk_name:
                    bitrate = int(chunk_name.split('Seg')[0])
                    logging.info(
                        f"{time.time():.3f} {download_time:.3f} "
                        f"{measured_tput:.3f} {self.current_throughput[client_id]:.3f} "
                        f"{bitrate} {chunk_name}"
                    )
            
            del self.chunk_start_times[client_id]
            
        return response_data
        
    def run(self):
        """Main event loop"""
        try:
            self.setup_server()
            
            while True:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    # Handle new connections
                    if fileno == self.server_socket.fileno():
                        client_socket, _ = self.server_socket.accept()
                        client_socket.setblocking(False)
                        self.epoll.register(client_socket.fileno(), select.EPOLLIN)
                        self.connections[client_socket.fileno()] = client_socket
                        self.requests[client_socket.fileno()] = b''
                        
                    # Handle client request data
                    elif event & select.EPOLLIN:
                        client_socket = self.connections[fileno]
                        request_data = client_socket.recv(65536)
                        
                        if request_data:
                            self.requests[fileno] += request_data
                            if b'\r\n\r\n' in request_data:  # Complete request
                                # Process request and connect to server
                                modified_request = self.handle_request(
                                    client_socket, 
                                    self.requests[fileno]
                                )
                                if modified_request:
                                    server_sock = socket.socket(
                                        socket.AF_INET, 
                                        socket.SOCK_STREAM
                                    )
                                    server_sock.connect((self.server_ip, self.server_port))
                                    server_sock.send(modified_request)
                                    
                                    # Register server socket for reading response
                                    server_sock.setblocking(False)
                                    self.epoll.register(
                                        server_sock.fileno(), 
                                        select.EPOLLIN
                                    )
                                    self.connections[server_sock.fileno()] = server_sock
                                    self.responses[server_sock.fileno()] = {
                                        'client': client_socket,
                                        'data': b''
                                    }
                        else:
                            # Client closed connection
                            self.epoll.unregister(fileno)
                            client_socket.close()
                            del self.connections[fileno]
                            del self.requests[fileno]
                            
                    # Handle server response data
                    elif event & select.EPOLLIN and fileno in self.responses:
                        server_socket = self.connections[fileno]
                        response_data = server_socket.recv(65536)
                        
                        if response_data:
                            self.responses[fileno]['data'] += response_data
                            if b'\r\n\r\n' in response_data:  # Complete response
                                # Process and forward response to client
                                processed_response = self.handle_response(
                                    self.responses[fileno]['client'],
                                    self.responses[fileno]['data']
                                )
                                self.responses[fileno]['client'].send(
                                    processed_response
                                )
                                
                                # Clean up server connection
                                self.epoll.unregister(fileno)
                                server_socket.close()
                                del self.connections[fileno]
                                del self.responses[fileno]
                        else:
                            # Server closed connection
                            self.epoll.unregister(fileno)
                            server_socket.close()
                            del self.connections[fileno]
                            del self.responses[fileno]
                            
        finally:
            self.epoll.close()
            self.server_socket.close()

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 proxy.py <log-file> <alpha> <port>")
        sys.exit(1)
        
    log_file = sys.argv[1]
    alpha = sys.argv[2]
    port = sys.argv[3]
    
    proxy = VideoProxy(log_file, alpha, port)
    proxy.run()

if __name__ == "__main__":
    main()