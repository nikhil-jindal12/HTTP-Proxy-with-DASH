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
        
        # Setup logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        # Filter out non-essential debug messages
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        # Connection management
        self.epoll = select.epoll()
        self.connections = {}
        self.requests = {}
        self.responses = {}
        
        # Video streaming state
        self.available_bitrates = []
        self.current_throughput = defaultdict(float)
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
        logging.info(f"Proxy server listening on port {self.port}")

    def create_server_request(self, path, headers):
        """Create a proper HTTP request to the server"""
        request = f"GET {path} HTTP/1.1\r\n"
        request += f"Host: {self.server_ip}\r\n"
        
        # Copy important headers from client request
        for header in headers:
            if not header:  # Skip empty lines
                continue
            if header.lower().startswith(('accept', 'user-agent', 'range')):
                request += f"{header}\r\n"
        
        # Add additional required headers
        request += "Connection: close\r\n"
        request += "Accept-Encoding: identity\r\n"  # Prevent gzip compression
        request += "\r\n"
        return request.encode('utf-8')

    def parse_http_request(self, request_data):
        """Parse HTTP request into lines while handling both CRLF and LF"""
        try:
            # Try UTF-8 first
            request_text = request_data.decode('utf-8')
        except UnicodeDecodeError:
            # If that fails, try ISO-8859-1
            request_text = request_data.decode('iso-8859-1')
        
        # Split on CRLF first, then LF if needed
        lines = request_text.split('\r\n')
        if len(lines) == 1:
            lines = request_text.split('\n')
        
        return [line for line in lines if line]  # Remove empty lines
    
    def parse_manifest(self, manifest_data):
        """Extract available bitrates from manifest file"""
        try:
            # Remove any potential BOM or whitespace
            manifest_data = manifest_data.strip()
            
            # Log the start of the manifest for debugging
            logging.debug(f"Parsing manifest starting with: {manifest_data[:100]}...")
            
            root = ET.fromstring(manifest_data)
            bitrates = []
            for rep in root.findall(".//{*}Representation"):
                try:
                    bitrate = int(rep.get('bandwidth')) // 1000  # Convert to Kbps
                    bitrates.append(bitrate)
                except (TypeError, ValueError) as e:
                    logging.error(f"Error parsing bitrate: {e}")
                    continue
                    
            if not bitrates:
                logging.warning("No bitrates found in manifest")
                return [1000]  # Return default bitrate if none found
                
            return sorted(bitrates)
        except ET.ParseError as e:
            logging.error(f"XML parsing error: {str(e)}")
            logging.error(f"Problematic XML content: {manifest_data}")
            return [1000]  # Return default bitrate on error
        
    def select_bitrate(self, client_id):
        """Select highest bitrate that current throughput can support"""
        current_tput = self.current_throughput[client_id]
        
        for bitrate in reversed(self.available_bitrates):
            if current_tput >= bitrate * 1.5:
                return bitrate
        
        return self.available_bitrates[0]  # Return lowest bitrate if none suitable

    def handle_request(self, client_socket, request_data):
        """Process incoming HTTP request"""
        try:
            request_lines = self.parse_http_request(request_data)
            if not request_lines:
                return None
            
            # Parse request line
            request_line = request_lines[0]
            headers = request_lines[1:]
            
            # Skip processing if this looks like a response
            if request_line.startswith('HTTP/'):
                return None
                
            logging.debug(f"Received request: {request_line}")
            
            try:
                method, path, version = request_line.split(' ')
            except ValueError:
                logging.error(f"Invalid request line: {request_line}")
                return None
    
            # For root path, redirect to index.html
            if path == "/" or not path:
                logging.debug("Root path requested, redirecting to index.html")
                path = "/index.html"
                
            # Handle index.html request directly
            if path == "/index.html":
                server_request = self.create_server_request(path, headers)
            # Handle video streaming requests
            elif 'manifest.mpd' in path or 'Seg' in path or 'init.mp4' in path:
                logging.debug(f"Handling video request: {path}")
                
                # Handle manifest file request
                if 'manifest.mpd' in path:
                    logging.debug("Handling manifest request")
                    if not self.manifest_cache:
                        # Fetch actual manifest from server
                        server_request = self.create_server_request(path, headers)
                        logging.debug(f"Sending manifest request to server: {server_request}")
                        
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
                                server_sock.connect((self.server_ip, self.server_port))
                                server_sock.send(server_request)
                                
                                manifest_data = b""
                                while True:
                                    chunk = server_sock.recv(4096)
                                    if not chunk:
                                        break
                                    manifest_data += chunk
                                
                                logging.debug(f"Received manifest response of size: {len(manifest_data)} bytes")
                                
                                if b'404 Not Found' in manifest_data:
                                    logging.error(f"Server returned 404 Not Found. Tried path: {path}")
                                    return None
                                
                                # Parse and cache manifest
                                self.manifest_cache = manifest_data
                                try:
                                    content_start = manifest_data.find(b'\r\n\r\n') + 4
                                    if content_start == 3:
                                        content_start = manifest_data.find(b'\n\n') + 2
                                    
                                    manifest_content = manifest_data[content_start:].decode('utf-8').strip()
                                    logging.debug(f"Manifest content: {manifest_content[:200]}...")
                                    
                                    self.available_bitrates = self.parse_manifest(manifest_content)
                                    logging.info(f"Available bitrates: {self.available_bitrates}")
                                except Exception as e:
                                    logging.error(f"Error parsing manifest: {e}")
                                    logging.error(f"Content start position: {content_start}")
                                    return None
                                    
                        except Exception as e:
                            logging.error(f"Error fetching manifest: {e}")
                            return None
                    
                    modified_path = path.replace('manifest.mpd', 'manifest_nolist.mpd')
                    server_request = self.create_server_request(modified_path, headers)
                    
                # Handle video chunk requests
                elif 'Seg' in path:
                    client_id = client_socket.fileno()
                    chunk_name = path.split('/')[-1]
                    
                    self.chunk_start_times[client_id] = time.time()
                    
                    try:
                        new_bitrate = self.select_bitrate(client_id)
                        current_bitrate = int(chunk_name.split('Seg')[0])
                        if current_bitrate != new_bitrate:
                            logging.debug(f"Modifying bitrate from {current_bitrate} to {new_bitrate}")
                            modified_path = path.replace(
                                f"{current_bitrate}Seg",
                                f"{new_bitrate}Seg"
                            )
                            server_request = self.create_server_request(modified_path, headers)
                        else:
                            server_request = self.create_server_request(path, headers)
                    except Exception as e:
                        logging.error(f"Error modifying bitrate: {e}")
                        server_request = self.create_server_request(path, headers)
                else:
                    # Handle init.mp4 requests
                    server_request = self.create_server_request(path, headers)
            else:
                # For all other requests, forward as is
                server_request = self.create_server_request(path, headers)
    
            # Create new connection to server
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.connect((self.server_ip, self.server_port))
            
            # Send the request
            total_sent = 0
            while total_sent < len(server_request):
                sent = server_sock.send(server_request[total_sent:])
                if sent == 0:
                    raise RuntimeError("Socket connection broken")
                total_sent += sent
    
            # Register server socket for reading response
            server_sock.setblocking(False)
            self.epoll.register(server_sock.fileno(), select.EPOLLIN)
            self.connections[server_sock.fileno()] = server_sock
            self.responses[server_sock.fileno()] = {
                'client': client_socket,
                'data': b'',
                'content_length': None,
                'headers_done': False,
                'total_received': 0
            }
            
            return None
                
        except Exception as e:
            logging.error(f"Error handling request: {e}")
            return None
    
    
    def cleanup_connection(self, fileno):
        """Clean up connection state"""
        try:
            if fileno in self.epoll.poll(0):
                self.epoll.unregister(fileno)
            
            if fileno in self.connections:
                try:
                    # Ensure all data is written before closing
                    if fileno in self.responses:
                        resp_data = self.responses[fileno].get('data', b'')
                        if resp_data:
                            try:
                                self.responses[fileno]['client'].send(resp_data)
                            except socket.error:
                                pass
                    
                    # Properly shutdown the socket
                    try:
                        self.connections[fileno].shutdown(socket.SHUT_RDWR)
                    except (socket.error, OSError):
                        pass
                        
                    self.connections[fileno].close()
                except (socket.error, OSError):
                    pass
                del self.connections[fileno]
            
            if fileno in self.requests:
                del self.requests[fileno]
            if fileno in self.responses:
                del self.responses[fileno]
            if fileno in self.chunk_start_times:
                del self.chunk_start_times[fileno]
                
        except Exception as e:
            logging.error(f"Error in cleanup_connection: {e}")

    def run(self):
        """Main event loop"""
        try:
            self.setup_server()
            
            while True:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    try:
                        # Handle new connections
                        if fileno == self.server_socket.fileno():
                            client_socket, addr = self.server_socket.accept()
                            client_socket.setblocking(False)
                            self.epoll.register(client_socket.fileno(), select.EPOLLIN)
                            self.connections[client_socket.fileno()] = client_socket
                            self.requests[client_socket.fileno()] = b''
                            logging.debug(f"New connection from {addr}")
                            
                        # Handle client request data
                        elif event & select.EPOLLIN and fileno in self.connections:
                            try:
                                data = self.connections[fileno].recv(65536)
                                if data:
                                    if fileno in self.requests:
                                        self.requests[fileno] += data
                                    else:
                                        self.requests[fileno] = data
                                        
                                    if b'\r\n\r\n' in self.requests[fileno]:
                                        self.handle_request(
                                            self.connections[fileno], 
                                            self.requests[fileno]
                                        )
                                else:
                                    self.cleanup_connection(fileno)
                            except socket.error as e:
                                if e.errno not in [errno.EAGAIN, errno.EWOULDBLOCK]:
                                    self.cleanup_connection(fileno)
                                    
                        # Handle server response data
                        elif event & select.EPOLLIN and fileno in self.responses:
                            try:
                                while True:  # Read all available data
                                    data = self.connections[fileno].recv(65536)
                                    if not data:
                                        break
                                        
                                    self.responses[fileno]['data'] += data
                                    self.responses[fileno]['total_received'] += len(data)
                                    
                                    # Try to send immediately
                                    try:
                                        sent = self.responses[fileno]['client'].send(data)
                                        if sent < len(data):
                                            # Buffer remaining data
                                            self.responses[fileno]['data'] = data[sent:]
                                    except socket.error as e:
                                        if e.errno not in [errno.EAGAIN, errno.EWOULDBLOCK]:
                                            raise
                                            
                                # If we got here with no data, connection is closed
                                if not self.responses[fileno]['data']:
                                    self.cleanup_connection(fileno)
                                    
                            except socket.error as e:
                                if e.errno not in [errno.EAGAIN, errno.EWOULDBLOCK]:
                                    self.cleanup_connection(fileno)
                                    
                    except Exception as e:
                        logging.error(f"Error in main event loop: {e}")
                        if fileno in self.connections:
                            self.cleanup_connection(fileno)
                            
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