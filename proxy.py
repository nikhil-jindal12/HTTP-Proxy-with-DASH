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
        
        # Setup logging with more detailed error information
        logging.basicConfig(
            level=logging.DEBUG,  # Change to DEBUG temporarily
            format='%(asctime)s %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler('debug.log'),
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
        self.current_throughput = defaultdict(lambda: 1000)
        self.chunk_start_times = {}
        self.chunk_sizes = {}
        self.manifest_cache = None
        
    def handle_socket_error(self, e, fileno, operation):
        """Centralized socket error handling"""
        if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
            # Non-blocking operation would block, just continue
            return False
        elif e.errno in [errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED]:
            # Connection reset or broken pipe, clean up quietly
            logging.debug(f"Connection reset during {operation}: {e}")
            self.cleanup_connection(fileno)
            return True
        else:
            # Log other errors and cleanup
            logging.error(f"Socket error during {operation}: {e}")
            self.cleanup_connection(fileno)
            return True

    def safe_send(self, sock, data):
        """Safely send data with error handling"""
        try:
            return sock.send(data)
        except socket.error as e:
            self.handle_socket_error(e, sock.fileno(), "send")
            return 0

    def safe_recv(self, sock, size):
        """Safely receive data with error handling"""
        try:
            return sock.recv(size)
        except socket.error as e:
            if self.handle_socket_error(e, sock.fileno(), "receive"):
                return None
            return b''

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
            if header.lower().startswith(('accept', 'user-agent', 'range', 'referer')):
                request += f"{header}\r\n"
        
        # Add important headers if not present
        if not any(h.lower().startswith('accept') for h in headers):
            request += "Accept: */*\r\n"
            
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
            content_start = manifest_data.find(b'\r\n\r\n') + 4
            if content_start < 4:
                return [1000]
                
            manifest_content = manifest_data[content_start:].decode('utf-8').strip()
            root = ET.fromstring(manifest_content)
            bitrates = []
            
            # Find all Representation elements and extract bandwidth
            for rep in root.findall(".//{*}Representation"):
                try:
                    bitrate = int(rep.get('bandwidth')) // 1000  # Convert to Kbps
                    bitrates.append(bitrate)
                except (TypeError, ValueError):
                    continue
                    
            return sorted(bitrates) if bitrates else [1000]
        except Exception as e:
            logging.error(f"Error parsing manifest: {e}")
            return [1000]
        
    def select_bitrate(self, client_id):
        """Select highest bitrate that current throughput can support"""
        current_tput = self.current_throughput[client_id]
        
        # Select highest bitrate where throughput is at least 1.5x the bitrate
        for bitrate in reversed(self.available_bitrates):
            if current_tput >= bitrate * 1.5:
                return bitrate
        
        return self.available_bitrates[0]  # Return lowest bitrate if none suitable

    def update_throughput(self, client_id, chunk_size, chunk_name):
        """Calculate and update throughput estimate using EWMA with improved error handling"""
        try:
            if client_id not in self.chunk_start_times:
                logging.debug(f"No start time found for client {client_id}")
                return
                
            end_time = time.time()
            start_time = self.chunk_start_times[client_id]
            duration = end_time - start_time
            
            if duration <= 0:
                logging.debug(f"Invalid duration ({duration}) for client {client_id}")
                return
                
            # Calculate throughput in Kbps
            chunk_throughput = (chunk_size * 8) / (duration * 1000)
            
            # Sanity check on throughput value
            if chunk_throughput <= 0 or chunk_throughput > 1000000:  # Max 1 Gbps
                logging.debug(f"Invalid throughput value ({chunk_throughput}) for client {client_id}")
                return
                
            # Update EWMA estimate
            prev_throughput = self.current_throughput[client_id]
            self.current_throughput[client_id] = (
                self.alpha * chunk_throughput + 
                (1 - self.alpha) * prev_throughput
            )
            
            # Extract bitrate from chunk name
            try:
                bitrate = int(chunk_name.split('Seg')[0])
            except (ValueError, IndexError):
                bitrate = 0
                logging.debug(f"Could not extract bitrate from chunk name: {chunk_name}")
            
            # Log chunk statistics
            logging.info(f"{time.time():.3f} {duration:.3f} {chunk_throughput:.3f} "
                        f"{self.current_throughput[client_id]:.3f} {bitrate} {chunk_name}")
                        
        except Exception as e:
            # Log the full context of the error
            logging.error(f"Error updating throughput for client {client_id}: {e}")
            logging.debug(f"Context - chunk_size: {chunk_size}, chunk_name: {chunk_name}, "
                    f"start_time exists: {client_id in self.chunk_start_times}")

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
                # Fetch actual manifest first if not cached
                if not self.manifest_cache:
                    manifest_path = path
                    server_request = self.create_server_request(manifest_path, headers)
                    
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        try:
                            s.settimeout(5)  # Add timeout
                            s.connect((self.server_ip, self.server_port))
                        except socket.error as e:
                            logging.error(f"Failed to connect to video server: {e}")
                            return None
                        
                        s.send(server_request)
                        
                        manifest_data = b""
                        while True:
                            chunk = s.recv(4096)
                            if not chunk:
                                break
                            manifest_data += chunk
                        
                        self.manifest_cache = manifest_data
                        self.available_bitrates = self.parse_manifest(manifest_data)
                        logging.info(f"Available bitrates: {self.available_bitrates}")
                
                # Replace with nolist manifest request
                path = path.replace('manifest.mpd', 'manifest_nolist.mpd')
                
            elif 'Seg' in path:
                # Handle video segment request
                client_id = client_socket.fileno()
                chunk_name = path.split('/')[-1]
                
                try:
                    current_bitrate = int(chunk_name.split('Seg')[0])
                    
                    # Record start time for throughput calculation
                    self.chunk_start_times[client_id] = time.time()
                    
                    # Select appropriate bitrate
                    new_bitrate = self.select_bitrate(client_id)
                    if current_bitrate != new_bitrate:
                        path = path.replace(f"{current_bitrate}Seg", f"{new_bitrate}Seg")
                        chunk_name = path.split('/')[-1]
                except ValueError:
                    pass
                
                # Store chunk name for logging
                if client_id not in self.responses:
                    self.responses[client_id] = {}
                self.responses[client_id]['chunk_name'] = chunk_name
            
            # Create server socket and send request
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.connect((self.server_ip, self.server_port))
            
            # Send the request
            server_request = self.create_server_request(path, headers)
            server_sock.send(server_request)
            
            # Setup for response handling
            server_sock.setblocking(False)
            server_fileno = server_sock.fileno()
            
            self.epoll.register(server_fileno, select.EPOLLIN)
            self.connections[server_fileno] = server_sock
            self.responses[server_fileno] = {
                'client': client_socket,
                'data': b'',
                'chunk_size': 0,
                'headers_parsed': False,
                'chunk_name': path.split('/')[-1] if 'Seg' in path else None
            }
            
            return None
                
        except Exception as e:
            logging.error(f"Error handling request: {e}")
            return None

    def parse_content_length(self, headers):
        """Extract Content-Length from response headers"""
        for line in headers.split(b'\r\n'):
            if line.lower().startswith(b'content-length:'):
                try:
                    return int(line.split(b': ')[1])
                except (IndexError, ValueError):
                    pass
        return None

    def run(self):
        """Main event loop with improved error handling"""
        try:
            self.setup_server()
            
            while True:
                try:
                    events = self.epoll.poll(1)
                    for fileno, event in events:
                        if fileno == self.server_socket.fileno():
                            try:
                                client_socket, addr = self.server_socket.accept()
                                client_socket.setblocking(False)
                                self.epoll.register(client_socket.fileno(), select.EPOLLIN)
                                self.connections[client_socket.fileno()] = client_socket
                                self.requests[client_socket.fileno()] = b''
                                logging.debug(f"New connection from {addr}")
                            except socket.error as e:
                                logging.error(f"Error accepting connection: {e}")
                                continue

                        elif event & select.EPOLLIN:
                            if fileno in self.requests:
                                # Handle client -> proxy data
                                data = self.safe_recv(self.connections[fileno], 8192)
                                if data is None:
                                    continue
                                
                                if data:
                                    self.requests[fileno] += data
                                    if b'\r\n\r\n' in self.requests[fileno]:
                                        try:
                                            self.handle_request(
                                                self.connections[fileno],
                                                self.requests[fileno]
                                            )
                                        except Exception as e:
                                            logging.error(f"Error handling request: {e}")
                                        self.requests[fileno] = b''
                                else:
                                    self.cleanup_connection(fileno)

                            elif fileno in self.responses:
                                # Handle server -> proxy data
                                data = self.safe_recv(self.connections[fileno], 8192)
                                if data is None:
                                    continue
                                
                                if data:
                                    response = self.responses[fileno]
                                    response['data'] += data
                                    
                                    if not response['headers_parsed']:
                                        if b'\r\n\r\n' in response['data']:
                                            headers = response['data'].split(b'\r\n\r\n')[0]
                                            response['chunk_size'] = self.parse_content_length(headers)
                                            response['headers_parsed'] = True
                                    
                                    if not self.safe_send(response['client'], data):
                                        self.cleanup_connection(fileno)
                                else:
                                    response = self.responses[fileno]
                                    if response.get('chunk_name') and 'Seg' in response['chunk_name']:
                                        try:
                                            self.update_throughput(
                                                fileno,
                                                response['chunk_size'] or len(response['data']),
                                                response['chunk_name']
                                            )
                                        except Exception as e:
                                            logging.error(f"Error updating throughput: {e}")
                                    self.cleanup_connection(fileno)

                        elif event & (select.EPOLLERR | select.EPOLLHUP):
                            # Handle error conditions
                            logging.debug(f"Socket {fileno} received error/hangup event")
                            self.cleanup_connection(fileno)

                except select.error as e:
                    if e.args[0] != errno.EINTR:
                        logging.error(f"Error in event loop: {e}")
                    continue
                
                except Exception as e:
                    logging.error(f"Unexpected error in event loop: {e}")
                    continue

        except Exception as e:
            logging.error(f"Fatal error in proxy: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up all resources"""
        try:
            # Close all connections
            for fileno in list(self.connections.keys()):
                self.cleanup_connection(fileno)
            
            # Close epoll and server socket
            if self.epoll:
                self.epoll.close()
            if hasattr(self, 'server_socket'):
                self.server_socket.close()
                
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")

    def cleanup_connection(self, fileno):
        """Enhanced connection cleanup"""
        try:
            # Unregister from epoll first
            try:
                if fileno in [fd for fd, _ in self.epoll.poll(0)]:
                    self.epoll.unregister(fileno)
            except (IOError, select.error) as e:
                logging.debug(f"Error unregistering from epoll: {e}")
            
            # Close socket
            if fileno in self.connections:
                sock = self.connections[fileno]
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except socket.error:
                    pass  # Socket might already be shut down
                try:
                    sock.close()
                except socket.error:
                    pass  # Socket might already be closed
                del self.connections[fileno]
            
            # Clean up other resources
            self.requests.pop(fileno, None)
            self.responses.pop(fileno, None)
            self.chunk_start_times.pop(fileno, None)
            self.current_throughput.pop(fileno, None)  # Clean up throughput data
            self.chunk_sizes.pop(fileno, None)  # Clean up chunk size data
            
        except Exception as e:
            logging.error(f"Error in cleanup_connection: {e}")

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