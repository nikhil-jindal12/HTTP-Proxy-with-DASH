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
    """
    This class is a proxy server that utilizes adaptive bitrate video streaming.
    """
    
    def __init__(self, log_file, alpha, port):
        """
        Initalizes the proxy with the following configurations

        Args:
            log_file (str): destination of the log file
            alpha (float): coefficient of 0-1
            port (int): port the proxy will listen on
        """
        self.alpha = float(alpha)
        self.port = int(port)
        self.server_ip = "149.165.170.233"
        self.server_port = 80
        
        # Performance log file
        self.log_file = log_file
        
        # Create the log file immediately to ensure it exists
        try:
            with open(self.log_file, 'w') as f:
                f.write("# Format: <time> <duration> <tput> <avg-tput> <bitrate> <chunkname>\n")
        except Exception as e:
            print(f"Error creating log file: {e}", file=sys.stderr)
        
        # Initialize connection
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
        
    def log_performance(self, duration, chunk_throughput, avg_throughput, bitrate, chunk_name):
        """
        Logs performance data to the log file

        Args:
            duration (float): duration of the chunk
            chunk_throughput (float): throughput of the chunk
            avg_throughput (float): average throughput
            bitrate (int): selected bitrate
            chunk_name (str): name of the chunk
        """
        try:
            with open(self.log_file, 'a') as f:
                f.write(f"{time.time():.3f} {duration:.3f} {chunk_throughput:.3f} {avg_throughput:.3f} {bitrate} {chunk_name}\n")
            print(f"Logged performance for chunk: {chunk_name}", file=sys.stderr)
        except Exception as e:
            # Print to stderr if logging fails
            print(f"Error logging performance: {e}", file=sys.stderr)
        
    def handle_socket_error(self, e, fileno, operation):
        """
        Centralize the error handling for socket errors

        Args:
            e (Exception): the exception to be handled
            fileno (int): fd of the socket
            operation (str): type of operation that failed

        Returns:
            bool: True if error was handled and connection should be closed
        """
        if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
            # Non-blocking operation would block
            return False
        elif e.errno in [errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED]:
            # Clean up if the connection is terminated by the client
            logging.debug(f"Connection reset during {operation}: {e}")
            self.cleanup_connection(fileno)
            return True
        else:
            # Unexpected error -> log error
            logging.error(f"Socket error during {operation}: {e}")
            self.cleanup_connection(fileno)
            return True

    def safe_send(self, sock, data):
        """
        Safely send data with error handling

        Args:
            sock (socket): socket to send data on
            data (bytes): data to send

        Returns:
            int: number of bytes sent, 0 o.w.
        """
        try:
            return sock.send(data)
        except socket.error as e:
            self.handle_socket_error(e, sock.fileno(), "send")
            return 0

    def safe_recv(self, sock, size):
        """
        Safely receive data with error handling

        Args:
            sock (socket): socket to receive data on
            size (int): max bytes to receive

        Returns:
            bytes: received data, None o.w.
        """
        try:
            return sock.recv(size)
        except socket.error as e:
            if self.handle_socket_error(e, sock.fileno(), "receive"):
                return None
            return b''

    def setup_server(self):
        """
        Set up the server socket and register it with epoll
        """
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('', self.port))
        server_socket.listen(1)
        server_socket.setblocking(False)
        
        self.epoll.register(server_socket.fileno(), select.EPOLLIN)
        self.server_socket = server_socket

    def create_server_request(self, path, headers):
        """
        Create a GET request for the server

        Args:
            path (str): path
            headers (list): request headers

        Returns:
            bytes: HTTP request
        """
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
        """
        Parse HTTP request to extract path and headers

        Args:
            request_data (bytes): raw HTTP request

        Returns:
            list: request lines
        """
        try:
            request_text = request_data.decode('utf-8')
            lines = request_text.split('\r\n')
            return [line for line in lines if line]
        except UnicodeDecodeError:
            return []
    
    def parse_manifest(self, manifest_data):
        """
        Parse manifest to extract available bitrates

        Args:
            manifest_data (bytes): raw manifest file

        Returns:
            list: available bitrates sorted by Kbps
        """
        try:
            # find the start of XML content
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
        """
        Choose the bitrate depending on the current throughput

        Args:
            client_id (int): identifier

        Returns:
            int: selected bitrate
        """
        current_tput = self.current_throughput[client_id]
        
        # Select highest bitrate where throughput is at least 1.5x the bitrate
        for bitrate in reversed(self.available_bitrates):
            if current_tput >= bitrate * 1.5:
                return bitrate
        
        return self.available_bitrates[0]  # Return lowest bitrate if none suitable

    def update_throughput(self, client_id, chunk_size, chunk_name, start_time):
        """
        Update throughput estimate

        Args:
            client_id (int): identifier
            chunk_size (int): size of chunk
            chunk_name (str): name of chunk
            start_time (float): timestamp when chunk download started
        """
        try:
            if start_time is None:
                print(f"No start time for chunk {chunk_name}", file=sys.stderr)
                return
                
            end_time = time.time()
            duration = end_time - start_time
            
            if duration <= 0:
                print(f"Invalid duration for chunk {chunk_name}: {duration}", file=sys.stderr)
                return
                
            # Calculate throughput in Kbps
            chunk_throughput = (chunk_size * 8) / (duration * 1000)
            
            # Validate throughput value
            if chunk_throughput <= 0 or chunk_throughput > 1000000:  # Max 1 Gbps
                print(f"Invalid throughput for chunk {chunk_name}: {chunk_throughput}", file=sys.stderr)
                return
                
            # Update estimate
            prev_throughput = self.current_throughput[client_id]
            self.current_throughput[client_id] = (
                self.alpha * chunk_throughput + 
                (1 - self.alpha) * prev_throughput
            )
            
            # Extract bitrate from chunk name
            try:
                bitrate = int(chunk_name.split('Seg')[0])
            except (ValueError, IndexError):
                print(f"Invalid chunk name format: {chunk_name}", file=sys.stderr)
                bitrate = 0
            
            # Log performance metrics
            self.log_performance(
                duration, 
                chunk_throughput, 
                self.current_throughput[client_id], 
                bitrate, 
                chunk_name
            )
                        
        except Exception as e:
            # Log detailed information about the error
            print(f"Error updating throughput for chunk {chunk_name}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)

    def handle_request(self, client_socket, request_data):
        """
        Process HTTP requests and forward to server

        Args:
            client_socket (socket): client's socket
            request_data (bytes): raw HTTP request data

        Returns:
            None
        """
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
                            s.settimeout(5)
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
                self.responses[client_id]['start_time'] = self.chunk_start_times.get(client_id)
            
            # Create server socket and send request
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.connect((self.server_ip, self.server_port))
            
            # Send the request to the server
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
                'chunk_name': path.split('/')[-1] if 'Seg' in path else None,
                'client_fileno': client_socket.fileno(),  # Store the client fileno
                'start_time': self.chunk_start_times.get(client_socket.fileno())  # Store the start time
            }
            
            return None
                
        except Exception as e:
            logging.error(f"Error handling request: {e}")
            return None

    def parse_content_length(self, headers):
        """
        Parse content length from headers

        Args:
            headers (bytes): raw HTTP headers

        Returns:
            int: content length, None o.w.
        """
        for line in headers.split(b'\r\n'):
            if line.lower().startswith(b'content-length:'):
                try:
                    return int(line.split(b': ')[1])
                except (IndexError, ValueError):
                    pass
        return None

    def run(self):
        """
        Run the proxy server
        """
        try:
            self.setup_server()
            
            while True:
                try:
                    # wait for events with a 1 sec timeout
                    events = self.epoll.poll(1)
                    for fileno, event in events:
                        if fileno == self.server_socket.fileno():
                            # Handle new client connection
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
                                    # gather requested data
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
                                    # Process and forward server's response
                                    response = self.responses[fileno]
                                    response['data'] += data
                                    
                                    # Parse add'l headers
                                    if not response['headers_parsed']:
                                        if b'\r\n\r\n' in response['data']:
                                            headers = response['data'].split(b'\r\n\r\n')[0]
                                            response['chunk_size'] = self.parse_content_length(headers)
                                            response['headers_parsed'] = True
                                    
                                    # Send data to client
                                    if not self.safe_send(response['client'], data):
                                        self.cleanup_connection(fileno)
                                else:
                                    # Server done sending responses
                                    response = self.responses[fileno]
                                    if response.get('chunk_name') and 'Seg' in response['chunk_name']:
                                        # Update throughput using the stored start time
                                        try:
                                            self.update_throughput(
                                                response['client_fileno'],  # Use client fileno instead
                                                response['chunk_size'] or len(response['data']),
                                                response['chunk_name'],
                                                response.get('start_time')  # Pass the stored start time
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
        """
        Cleanup all resources
        """
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
        """
        Cleanup a connection

        Args:
            fileno (int): fd to be cleaned up
        """
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
            self.current_throughput.pop(fileno, None)
            self.chunk_sizes.pop(fileno, None)
            
        except Exception as e:
            logging.error(f"Error in cleanup_connection: {e}")

def main():
    """
    Validates command line arguments and starts proxy.
    """
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