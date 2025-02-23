#!/usr/bin/env python3
import sys
import socket
import select
import time
import xml.etree.ElementTree as ET
import logging
from collections import defaultdict
import errno

class VideoProxy:
    def __init__(self, log_file, alpha, port):
        self.alpha = float(alpha)
        self.port = int(port)
        self.server_ip = "149.165.170.233"
        self.server_port = 80
        
        # Setup logging to only include required fields
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        # Connection management
        self.epoll = select.epoll()
        self.connections = {}
        self.requests = {}
        self.responses = {}
        
        # Video streaming state
        self.available_bitrates = []
        self.current_throughput = defaultdict(float)
        self.chunk_start_times = {}
        self.chunk_sizes = {}
        self.manifest_cache = None

    def setup_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('', self.port))
        server_socket.listen(1)
        server_socket.setblocking(False)
        
        self.epoll.register(server_socket.fileno(), select.EPOLLIN)
        self.server_socket = server_socket

    def create_server_request(self, path, headers):
        """Create a proper HTTP request to the server"""
        request = f"GET {path} HTTP/1.1\r\n"
        request += f"Host: {self.server_ip}\r\n"
        
        # Copy important headers from client request
        for header in headers:
            if header.lower().startswith(('accept', 'user-agent', 'range')):
                request += f"{header}\r\n"
        
        request += "Connection: close\r\n\r\n"
        return request.encode('utf-8')

    def parse_http_request(self, request_data):
        """Parse HTTP request into lines"""
        try:
            request_text = request_data.decode('utf-8')
            lines = request_text.split('\r\n')
            return [line for line in lines if line]
        except UnicodeDecodeError:
            return []
    
    def parse_manifest(self, manifest_data):
        """Extract available bitrates from manifest file"""
        try:
            manifest_data = manifest_data.strip()
            root = ET.fromstring(manifest_data)
            bitrates = []
            
            # Find all Representation elements and extract bandwidth
            for rep in root.findall(".//{*}Representation"):
                try:
                    bitrate = int(rep.get('bandwidth')) // 1000  # Convert to Kbps
                    bitrates.append(bitrate)
                except (TypeError, ValueError):
                    continue
                    
            return sorted(bitrates) if bitrates else [1000]
        except ET.ParseError:
            return [1000]
        
    def select_bitrate(self, client_id):
        """Select highest bitrate that current throughput can support"""
        current_tput = self.current_throughput[client_id]
        
        # Select highest bitrate where throughput is at least 1.5x the bitrate
        for bitrate in reversed(self.available_bitrates):
            if current_tput >= bitrate * 1.5:
                return bitrate
        
        return self.available_bitrates[0]  # Return lowest bitrate if none suitable

    def update_throughput(self, client_id, chunk_size):
        """Calculate and update throughput estimate using EWMA"""
        end_time = time.time()
        start_time = self.chunk_start_times[client_id]
        duration = end_time - start_time
        
        # Calculate throughput in Kbps
        chunk_throughput = (chunk_size * 8) / (duration * 1000)
        
        # Update EWMA estimate
        self.current_throughput[client_id] = (
            self.alpha * chunk_throughput + 
            (1 - self.alpha) * self.current_throughput[client_id]
        )
        
        # Log chunk statistics
        chunk_name = self.responses[client_id]['chunk_name'] if client_id in self.responses else "unknown"
        bitrate = int(chunk_name.split('Seg')[0]) if 'Seg' in chunk_name else 0
        
        logging.info(f"{time.time():.3f} {duration:.3f} {chunk_throughput:.3f} "
                    f"{self.current_throughput[client_id]:.3f} {bitrate} {chunk_name}")

    def handle_request(self, client_socket, request_data):
        """Process incoming HTTP request"""
        try:
            request_lines = self.parse_http_request(request_data)
            if not request_lines:
                return None
                
            method, path, version = request_lines[0].split(' ')
            headers = request_lines[1:]
            
            # Handle different request types
            if path == "/" or not path:
                path = "/index.html"
                
            if 'manifest.mpd' in path:
                # Replace manifest request with nolist version
                if not self.manifest_cache:
                    # Fetch and parse actual manifest first
                    manifest_path = path
                    server_request = self.create_server_request(manifest_path, headers)
                    
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.server_ip, self.server_port))
                        s.send(server_request)
                        
                        manifest_data = b""
                        while True:
                            chunk = s.recv(4096)
                            if not chunk:
                                break
                            manifest_data += chunk
                        
                        # Parse manifest content
                        content_start = manifest_data.find(b'\r\n\r\n') + 4
                        manifest_content = manifest_data[content_start:].decode('utf-8')
                        self.available_bitrates = self.parse_manifest(manifest_content)
                        self.manifest_cache = manifest_data
                
                # Send nolist manifest request
                path = path.replace('manifest.mpd', 'manifest_nolist.mpd')
                
            elif 'Seg' in path:
                # Handle video segment request
                client_id = client_socket.fileno()
                chunk_name = path.split('/')[-1]
                current_bitrate = int(chunk_name.split('Seg')[0])
                
                # Record start time for throughput calculation
                self.chunk_start_times[client_id] = time.time()
                
                # Select appropriate bitrate
                new_bitrate = self.select_bitrate(client_id)
                if current_bitrate != new_bitrate:
                    path = path.replace(f"{current_bitrate}Seg", f"{new_bitrate}Seg")
                
                # Store chunk name for logging
                if client_id not in self.responses:
                    self.responses[client_id] = {}
                self.responses[client_id]['chunk_name'] = path.split('/')[-1]
            
            # Create and send server request
            server_request = self.create_server_request(path, headers)
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.connect((self.server_ip, self.server_port))
            server_sock.setblocking(False)
            
            # Register for epoll
            self.epoll.register(server_sock.fileno(), select.EPOLLIN)
            self.connections[server_sock.fileno()] = server_sock
            self.responses[server_sock.fileno()] = {
                'client': client_socket,
                'data': b'',
                'chunk_size': 0,
                'headers_parsed': False
            }
            
            server_sock.send(server_request)
            return None
                
        except Exception as e:
            logging.error(f"Error handling request: {e}")
            return None

    def run(self):
        """Main event loop"""
        try:
            self.setup_server()
            
            while True:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    try:
                        if fileno == self.server_socket.fileno():
                            # Accept new connection
                            client_socket, _ = self.server_socket.accept()
                            client_socket.setblocking(False)
                            self.epoll.register(client_socket.fileno(), select.EPOLLIN)
                            self.connections[client_socket.fileno()] = client_socket
                            self.requests[client_socket.fileno()] = b''
                            
                        elif event & select.EPOLLIN:
                            # Handle incoming data
                            if fileno in self.requests:
                                # Client -> Proxy data
                                data = self.connections[fileno].recv(8192)
                                if data:
                                    self.requests[fileno] += data
                                    if b'\r\n\r\n' in self.requests[fileno]:
                                        self.handle_request(
                                            self.connections[fileno],
                                            self.requests[fileno]
                                        )
                                else:
                                    self.cleanup_connection(fileno)
                            else:
                                # Server -> Proxy data
                                while True:
                                    try:
                                        data = self.connections[fileno].recv(8192)
                                        if not data:
                                            break
                                            
                                        response = self.responses[fileno]
                                        response['data'] += data
                                        
                                        # Parse content length from headers
                                        if not response['headers_parsed']:
                                            if b'\r\n\r\n' in response['data']:
                                                headers = response['data'].split(b'\r\n\r\n')[0]
                                                for line in headers.split(b'\r\n'):
                                                    if b'Content-Length:' in line:
                                                        response['chunk_size'] = int(line.split(b': ')[1])
                                                response['headers_parsed'] = True
                                        
                                        # Forward data to client
                                        response['client'].send(data)
                                        
                                    except socket.error as e:
                                        if e.errno not in [errno.EAGAIN, errno.EWOULDBLOCK]:
                                            raise
                                        break
                                        
                                # Check if response is complete
                                if len(response['data']) >= response['chunk_size'] and response['headers_parsed']:
                                    if 'Seg' in self.responses[fileno].get('chunk_name', ''):
                                        self.update_throughput(fileno, response['chunk_size'])
                                    self.cleanup_connection(fileno)
                                    
                    except Exception as e:
                        logging.error(f"Error in event loop: {e}")
                        if fileno in self.connections:
                            self.cleanup_connection(fileno)
                            
        finally:
            self.epoll.close()
            self.server_socket.close()

    def cleanup_connection(self, fileno):
        """Clean up connection state"""
        try:
            if fileno in self.epoll.poll(0):
                self.epoll.unregister(fileno)
            
            if fileno in self.connections:
                self.connections[fileno].close()
                del self.connections[fileno]
            
            if fileno in self.requests:
                del self.requests[fileno]
            if fileno in self.responses:
                del self.responses[fileno]
            if fileno in self.chunk_start_times:
                del self.chunk_start_times[fileno]
                
        except Exception as e:
            logging.error(f"Error in cleanup: {e}")

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