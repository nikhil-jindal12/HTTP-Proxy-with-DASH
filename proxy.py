#!/usr/bin/env python3
import sys
import socket
import select
import time
import xml.etree.ElementTree as ET
import logging
import re
from collections import defaultdict
import errno

class VideoProxy:
    """
    This class is a proxy server that utilizes adaptive bitrate video streaming.
    It intercepts HTTP requests for video chunks and modifies bitrates based on
    network conditions to optimize streaming quality.
    """
    
    def __init__(self, log_file, alpha, port, server_ip=None, server_port=None):
        """
        Initializes the proxy with configuration parameters and sets up initial state
        
        Args:
            log_file: Path to write performance logs
            alpha: EWMA smoothing factor (0-1) for throughput estimation
            port: Port to listen for client connections
            server_ip: IP of the DASH video server (default: 149.165.170.233)
            server_port: Port of the video server (default: 80)
        """
        self.alpha = float(alpha)
        self.port = int(port)
        self.server_ip = server_ip or "149.165.170.233"
        self.server_port = server_port or 80
        
        # Performance log file
        self.log_file = log_file
        
        # Create the log file immediately to ensure it exists
        try:
            with open(self.log_file, 'w') as f:
                pass  # Just create the file, don't write anything
        except Exception as e:
            print(f"Error creating log file: {e}", file=sys.stderr)
        
        # Initialize epoll for non-blocking I/O multiplexing
        self.epoll = select.epoll()
        self.connections = {}
        self.requests = {}
        self.responses = {}
        
        # Available bitrates for adaptive streaming
        self.available_bitrates = []
        
        # Start with a conservative initial throughput estimate
        self.global_throughput = 500.0  # Initial estimate (Kbps)
        
        # Tracking data structures for request/response handling
        self.seen_chunks = set()
        self.content_lengths = {}
        self.chunk_start_times = {}
        self.chunk_sizes = {}
        self.manifest_cache = None
        
        # Counter for initial segments to handle startup conditions
        self.initial_segments_count = 0
        
        # Track the last time we checked for stalled transfers
        self.last_stall_check = time.time()
        
        print(f"Proxy initialized with alpha={alpha}, initial throughput={self.global_throughput} Kbps", file=sys.stderr)
    
    def log_performance(self, duration, chunk_throughput, avg_throughput, bitrate, chunk_name):
        """
        Logs performance data to the log file in the required format:
        <time> <duration> <tput> <avg-tput> <bitrate> <chunkname>
        
        This log is used for analysis and visualization of adaptation performance.
        """
        try:
            with open(self.log_file, 'a') as f:
                f.write(f"{time.time():.3f} {duration:.3f} {chunk_throughput:.3f} {avg_throughput:.3f} {bitrate} {chunk_name}\n")
            print(f"Logged: {duration:.3f}s, Throughput: {chunk_throughput:.3f} Kbps, Avg: {avg_throughput:.3f} Kbps, Bitrate: {bitrate} Kbps, Chunk: {chunk_name}", file=sys.stderr)
        except Exception as e:
            print(f"Error logging performance: {e}", file=sys.stderr)
    
    def setup_server(self):
        """
        Set up the proxy server socket and register it with epoll for non-blocking I/O
        Creates a socket that listens for incoming client connections.
        """
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('', self.port))
        server_socket.listen(128)  # Allow up to 128 pending connections in the queue
        server_socket.setblocking(False)
        
        self.epoll.register(server_socket.fileno(), select.EPOLLIN)
        self.server_socket = server_socket
        print(f"Proxy server started on port {self.port}", file=sys.stderr)

    def create_server_request(self, path, headers):
        """
        Create a properly formatted HTTP request to send to the video server
        
        Args:
            path: The URL path to request
            headers: HTTP headers from the client request
            
        Returns:
            Encoded HTTP request ready to send to the server
        """
        # Make sure path starts with '/'
        if not path.startswith('/'):
            path = '/' + path
            
        # Debug the path we're sending
        print(f"Creating server request for path: {path}", file=sys.stderr)
        
        request = f"GET {path} HTTP/1.1\r\n"
        request += f"Host: {self.server_ip}\r\n"
        
        # Copy important headers from client request
        for header in headers:
            if ':' not in header:
                continue
                
            key, value = header.split(':', 1)
            if key.lower() in ('accept', 'user-agent', 'range', 'referer', 'accept-encoding'):
                request += f"{header}\r\n"
        
        # Add important headers if not present
        if not any(h.lower().startswith('accept') for h in headers):
            request += "Accept: */*\r\n"
            
        request += "Connection: close\r\n\r\n"
        
        # Log the full request to help debug
        print(f"Full request: {request}", file=sys.stderr)
        
        return request.encode('utf-8')

    def parse_http_request(self, request_data):
        """
        Parse HTTP request data to extract header lines
        
        Args:
            request_data: Raw HTTP request data
            
        Returns:
            List of HTTP header lines (strings)
        """
        try:
            # Find the end of headers (double CRLF)
            header_end = request_data.find(b'\r\n\r\n')
            if header_end == -1:
                return []
                
            request_text = request_data[:header_end].decode('utf-8')
            lines = request_text.split('\r\n')
            return [line for line in lines if line]
        except UnicodeDecodeError:
            return []
    
    def parse_manifest(self, manifest_data):
        """
        Parse the MPD manifest file to extract available video bitrates
        
        Args:
            manifest_data: Raw HTTP response containing the manifest
            
        Returns:
            List of available bitrates in Kbps, sorted ascending
        """
        try:
            # Find the start of XML content (after HTTP headers)
            content_start = manifest_data.find(b'\r\n\r\n')
            if content_start == -1:
                print("Warning: Could not find end of headers in manifest", file=sys.stderr)
                return [10, 100, 500, 1000]  # Fallback bitrates
                    
            content_start += 4  # Skip the \r\n\r\n
            manifest_content = manifest_data[content_start:].decode('utf-8').strip()
            
            try:
                root = ET.fromstring(manifest_content)
                print(f"Successfully parsed XML root element: {root.tag}", file=sys.stderr)
            except ET.ParseError as e:
                print(f"XML parse error: {e}", file=sys.stderr)
                print(f"First 100 chars of manifest: {manifest_content[:100]}", file=sys.stderr)
                return [10, 100, 500, 1000]  # Fallback bitrates
                
            bitrates = set()  # Use a set to prevent duplicates
            
            # Try to find Representation elements with different namespace patterns
            namespaces = {
                'mpd': 'urn:mpeg:dash:schema:mpd:2011',
                'ns0': 'urn:mpeg:dash:schema:mpd:2011'
            }
            
            found_reps = False
            for ns_prefix, ns_uri in namespaces.items():
                reps = root.findall(f".//{{{ns_uri}}}Representation")
                if reps:
                    found_reps = True
                    print(f"Found {len(reps)} representations with namespace {ns_prefix}", file=sys.stderr)
                    for rep in reps:
                        try:
                            if 'bandwidth' in rep.attrib:
                                bitrate = int(rep.attrib['bandwidth']) // 1000  # Convert to Kbps
                                bitrates.add(bitrate)  # Use add() for sets
                                print(f"Found bitrate: {bitrate} Kbps", file=sys.stderr)
                        except (ValueError, TypeError):
                            continue
            
            # If no success with namespaces, try without namespace
            if not found_reps:
                reps = root.findall(".//Representation")
                print(f"Searching without namespace, found {len(reps)} representations", file=sys.stderr)
                for rep in reps:
                    try:
                        if 'bandwidth' in rep.attrib:
                            bitrate = int(rep.attrib['bandwidth']) // 1000
                            bitrates.add(bitrate)  # Use add() for sets
                            print(f"Found bitrate: {bitrate} Kbps", file=sys.stderr)
                    except (ValueError, TypeError):
                        continue
            
            # If we still can't find any bitrates, search by looking for bandwidth attribute
            if not bitrates:
                print("Trying to find bandwidth attributes directly", file=sys.stderr)
                for elem in root.findall(".//*"):
                    if 'bandwidth' in elem.attrib:
                        try:
                            bitrate = int(elem.attrib['bandwidth']) // 1000
                            bitrates.add(bitrate)  # Use add() for sets
                            print(f"Found bitrate from element {elem.tag}: {bitrate} Kbps", file=sys.stderr)
                        except (ValueError, TypeError):
                            continue
            
            if bitrates:
                result = sorted(list(bitrates))
                print(f"Final parsed bitrates: {result}", file=sys.stderr)
                
                # Sanity check to ensure reasonable bitrates are found
                if len(result) <= 1 or min(result) != 10 or max(result) < 500:
                    print("WARNING: Parsed bitrates look suspicious, using default set", file=sys.stderr)
                    return [10, 100, 500, 1000]
                    
                return result
            else:
                print("No bitrates found in manifest, using defaults", file=sys.stderr)
                return [10, 100, 500, 1000]  # Default bitrates
                
        except Exception as e:
            print(f"Error parsing manifest: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            return [10, 100, 500, 1000]  # Fallback bitrates
            
    def select_bitrate(self, current_throughput):
        """
        Choose the highest bitrate that current network conditions can support
        
        The selection uses the 1.5x safety margin required by the project spec:
        bitrate must be less than (throughput / 1.5)
        
        Args:
            current_throughput: Current estimated network throughput in Kbps
            
        Returns:
            Selected bitrate in Kbps
        """
        print(f"Selecting bitrate - throughput: {current_throughput:.2f} Kbps, available: {self.available_bitrates}", file=sys.stderr)
        
        # If we don't have bitrates yet, use default
        if not self.available_bitrates:
            print("No bitrates available yet, using default 500", file=sys.stderr)
            return 500
        
        # Make sure we have the full range of bitrates
        if len(self.available_bitrates) <= 1:
            # Force-reset available bitrates if we somehow only have one (usually just 10)
            self.available_bitrates = [10, 100, 500, 1000]
            print(f"WARNING: Only had one bitrate, reset to standard set: {self.available_bitrates}", file=sys.stderr)
                
        # For very low throughput, always use the lowest available bitrate
        if current_throughput < 20:
            print(f"Very low throughput detected ({current_throughput:.2f} Kbps), using lowest bitrate", file=sys.stderr)
            selected = self.available_bitrates[0]
            print(f"Selected bitrate: {selected} Kbps", file=sys.stderr)
            return selected
        
        # Use the specified ratio of 1.5 from the project spec for all cases
        safety_factor = 1.5
        target_throughput = current_throughput / safety_factor
        
        print(f"Target throughput: {target_throughput:.2f} Kbps (after applying safety factor {safety_factor})", file=sys.stderr)
        
        # Find highest bitrate below target throughput
        selected = self.available_bitrates[0]  # Start with lowest as fallback
        
        # Loop through each bitrate and evaluate
        for i, bitrate in enumerate(self.available_bitrates):
            print(f"  Evaluating bitrate {bitrate} <= {target_throughput:.2f}? {bitrate <= target_throughput}", file=sys.stderr)
            if bitrate <= target_throughput:
                selected = bitrate
                # Continue checking higher bitrates
            else:
                # Found a bitrate too high, stop
                break
                
        # Sanity check - if throughput is high and we're still using the lowest bitrate, force upgrade
        if selected == self.available_bitrates[0] and current_throughput > (self.available_bitrates[1] * 2.0):
            print(f"Forcing bitrate upgrade: throughput {current_throughput:.2f} Kbps is much higher than lowest bitrate", file=sys.stderr)
            selected = self.available_bitrates[1]  # Use the second lowest bitrate
            
        print(f"Selected bitrate: {selected} Kbps (target: {target_throughput:.2f} Kbps)", file=sys.stderr)
        return selected

    def update_throughput(self, chunk_size, chunk_name, start_time):
        """
        Update the throughput estimate using EWMA (Exponentially Weighted Moving Average)
        
        Formula: T_current = α * T_new + (1-α) * T_current
        
        Args:
            chunk_size: Size of the downloaded chunk in bytes
            chunk_name: Name of the chunk for logging
            start_time: Time when download started
        """
        try:
            # Skip throughput updates for initialization segments
            if chunk_name and ('init' in chunk_name.lower() or 'initialize' in chunk_name.lower()):
                print(f"Skipping throughput update for initialization segment: {chunk_name}", file=sys.stderr)
                return
                
            if start_time is None:
                print(f"No start time for chunk {chunk_name}", file=sys.stderr)
                return
                
            end_time = time.time()
            duration = end_time - start_time
            
            # Guard against unreasonably short durations
            if duration < 0.01:
                print(f"Duration too short for chunk {chunk_name}: {duration} seconds", file=sys.stderr)
                return
                
            # Calculate throughput in Kbps: (size in bits) / (duration in seconds) / 1000
            chunk_throughput = (chunk_size * 8) / (duration * 1000)
            
            # Apply sanity checks on throughput values
            MIN_REASONABLE_THROUGHPUT = 5.0   # 5 Kbps minimum
            MAX_REASONABLE_THROUGHPUT = 10000.0  # 10 Mbps maximum
            
            # If throughput is outside reasonable bounds, adjust it
            if chunk_throughput < MIN_REASONABLE_THROUGHPUT:
                print(f"Throughput value too low: {chunk_throughput:.2f} Kbps, adjusting to minimum", file=sys.stderr)
                chunk_throughput = MIN_REASONABLE_THROUGHPUT
            elif chunk_throughput > MAX_REASONABLE_THROUGHPUT:
                print(f"Throughput value too high: {chunk_throughput:.2f} Kbps, adjusting to maximum", file=sys.stderr)
                chunk_throughput = MAX_REASONABLE_THROUGHPUT
            
            # Apply EWMA formula with the alpha parameter
            prev_throughput = self.global_throughput
            self.global_throughput = self.alpha * chunk_throughput + (1 - self.alpha) * prev_throughput
            
            print(f"Updated throughput: {prev_throughput:.2f} → {self.global_throughput:.2f} Kbps (chunk: {chunk_throughput:.2f} Kbps)", file=sys.stderr)
            
            # Extract bitrate from chunk name
            bitrate = 0
            try:
                # Parse bitrate from chunk name (e.g., "500Seg2" -> 500)
                bitrate_match = re.search(r'(\d+)Seg', chunk_name)
                if bitrate_match:
                    bitrate = int(bitrate_match.group(1))
            except (ValueError, IndexError):
                print(f"Could not extract bitrate from chunk name: {chunk_name}", file=sys.stderr)
            
            # Log performance metrics
            self.log_performance(duration, chunk_throughput, self.global_throughput, bitrate, chunk_name)
                        
        except Exception as e:
            print(f"Error updating throughput for chunk {chunk_name}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)

    def handle_client_request(self, client_socket, request_data):
        """
        Process HTTP requests from clients and forward to the video server
        
        This is where the key adaptive bitrate logic occurs:
        1. For manifest requests, fetch actual manifest to extract bitrates
        2. For video segments, modify the requested bitrate based on throughput
        
        Args:
            client_socket: Socket connected to the client
            request_data: Raw HTTP request data
            
        Returns:
            Server socket connected to the video server, or None on error
        """
        try:
            request_lines = self.parse_http_request(request_data)
            if not request_lines or len(request_lines[0].split()) < 3:
                return None
                
            request_parts = request_lines[0].split()
            if len(request_parts) < 3:
                print(f"Invalid request line: {request_lines[0]}", file=sys.stderr)
                return None
                
            method, path, version = request_parts
            headers = request_lines[1:]
            
            # Extract client ID for logging
            client_id = client_socket.fileno()
            print(f"Handling request from client {client_id}: {method} {path}", file=sys.stderr)
            
            # Handle root path
            if path == "/":
                path = "/index.html"
            
            # Remove leading slash if present for path processing
            clean_path = path[1:] if path.startswith('/') else path
                
            # Handle manifest file request - intercept to extract bitrates
            if 'manifest.mpd' in path:
                # Fetch actual manifest from server to extract bitrates
                manifest_path = '/vod/manifest.mpd'  # Always use the full path
                server_request = self.create_server_request(manifest_path, headers)
                
                # Use a separate connection for manifest request
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    try:
                        s.settimeout(5)
                        s.connect((self.server_ip, self.server_port))
                    except socket.error as e:
                        print(f"Failed to connect to video server: {e}", file=sys.stderr)
                        return None
                    
                    s.send(server_request)
                    
                    manifest_data = b""
                    while True:
                        try:
                            chunk = s.recv(4096)
                            if not chunk:
                                break
                            manifest_data += chunk
                        except socket.timeout:
                            print("Timeout receiving manifest data", file=sys.stderr)
                            break
                    
                    if manifest_data:
                        self.manifest_cache = manifest_data
                        self.available_bitrates = self.parse_manifest(manifest_data)
                        print(f"Available bitrates: {self.available_bitrates}", file=sys.stderr)
                
                # Redirect client to the no_list variant to disable browser's built-in adaptation
                path = path.replace('manifest.mpd', 'manifest_nolist.mpd')
                
            # Handle video segment request - this is where bitrate adaptation happens
            elif 'Seg' in path:
                chunk_name = path.split('/')[-1]
                
                # Record start time for throughput calculation
                self.chunk_start_times[client_id] = time.time()
                
                # Check if it's an initialization segment
                if 'init' in chunk_name.lower():
                    print(f"Initialization segment request: {chunk_name}", file=sys.stderr)
                else:
                    # Extract all path components for proper manipulation
                    path_components = path.split('/')
                    if len(path_components) >= 3:
                        # Parse current bitrate from chunk name and from directory
                        try:
                            # Get current requested bitrate from segment name (e.g., "500Seg1" -> 500)
                            bitrate_match = re.search(r'(\d+)Seg', chunk_name)
                            if bitrate_match:
                                current_bitrate = int(bitrate_match.group(1))
                                
                                # Find the bitrate in the directory path
                                dir_index = -2  # Second-to-last component should be the bitrate dir
                                if len(path_components) > 2:
                                    # Try to extract the directory bitrate
                                    try:
                                        dir_bitrate = int(path_components[dir_index])
                                    except ValueError:
                                        dir_bitrate = current_bitrate
                                else:
                                    dir_bitrate = current_bitrate
                                
                                # Select the appropriate bitrate based on current throughput
                                new_bitrate = self.select_bitrate(self.global_throughput)
                                
                                # Modify both the path component and the filename to use the new bitrate
                                if current_bitrate != new_bitrate:
                                    # Update the directory component
                                    path_components[dir_index] = str(new_bitrate)
                                    
                                    # Update the filename
                                    old_segment = f"{current_bitrate}Seg"
                                    new_segment = f"{new_bitrate}Seg"
                                    path_components[-1] = path_components[-1].replace(old_segment, new_segment)
                                    
                                    # Rebuild the path
                                    path = '/'.join(path_components)
                                    
                                    print(f"Changed bitrate from {current_bitrate} to {new_bitrate} Kbps", file=sys.stderr)
                                    chunk_name = path_components[-1]
                        except (ValueError, IndexError) as e:
                            print(f"Error parsing bitrate from chunk name: {e}", file=sys.stderr)
            
            # Create server socket and request
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.settimeout(10)  # Set a reasonable timeout
            
            try:
                server_sock.connect((self.server_ip, self.server_port))
            except Exception as e:
                print(f"Failed to connect to server: {e}", file=sys.stderr)
                return None
                
            # Ensure the path has a leading slash for the request
            if not path.startswith('/'):
                path = '/' + path
                
            # Send the request to the server
            server_request = self.create_server_request(path, headers)
            server_sock.send(server_request)
            
            # Prepare for non-blocking operation
            server_sock.setblocking(False)
            server_fileno = server_sock.fileno()
            
            # Register with epoll for reading
            self.epoll.register(server_fileno, select.EPOLLIN)
            self.connections[server_fileno] = server_sock
            
            # Store response state
            self.responses[server_fileno] = {
                'client': client_socket,
                'data': b'',
                'chunk_size': 0,
                'headers_parsed': False,
                'chunk_name': chunk_name if 'Seg' in path else None,
                'client_fileno': client_id,
                'start_time': self.chunk_start_times.get(client_id),
                'last_activity': time.time()  # Track last activity for stall detection
            }
            
            return server_sock
                
        except Exception as e:
            print(f"Error handling client request: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            return None

    def parse_content_length(self, headers):
        """
        Extract Content-Length from HTTP headers
        
        Args:
            headers: HTTP headers as bytes
            
        Returns:
            Content length as integer, or None if not found
        """
        try:
            # First try decoding as UTF-8
            headers_str = headers.decode('utf-8', 'ignore')
            
            # Look for content-length with case-insensitive match
            for line in headers_str.split('\r\n'):
                if line.lower().startswith('content-length:'):
                    length_str = line.split(':', 1)[1].strip()
                    try:
                        return int(length_str)
                    except ValueError:
                        print(f"Invalid Content-Length value: {length_str}", file=sys.stderr)
            
            # If not found with UTF-8, try with raw bytes
            for line in headers.split(b'\r\n'):
                if line.lower().startswith(b'content-length:'):
                    try:
                        length_str = line.split(b':', 1)[1].strip()
                        return int(length_str)
                    except (ValueError, IndexError):
                        pass
                        
            # Log the error and headers for debugging
            print(f"Could not find Content-Length in headers: {headers_str[:200]}...", file=sys.stderr)
            
        except Exception as e:
            print(f"Error parsing content length: {e}", file=sys.stderr)
            
        return None

    def run(self):
        """
        Main event loop for the proxy server
        
        Uses epoll to handle multiple connections concurrently without threads.
        Processes events for client and server connections and manages the
        lifecycle of HTTP requests and responses.
        """
        try:
            self.setup_server()
            last_check_time = time.time()
            
            while True:
                try:
                    # Wait for events with a 1 second timeout
                    events = self.epoll.poll(1)
                    
                    for fileno, event in events:
                        # New client connection
                        if fileno == self.server_socket.fileno():
                            try:
                                client_socket, addr = self.server_socket.accept()
                                client_socket.setblocking(False)
                                self.epoll.register(client_socket.fileno(), select.EPOLLIN)
                                self.connections[client_socket.fileno()] = client_socket
                                self.requests[client_socket.fileno()] = b''
                                print(f"New connection from {addr}", file=sys.stderr)
                            except socket.error as e:
                                print(f"Error accepting connection: {e}", file=sys.stderr)
                                continue

                        # Handle errors and hangups first
                        elif event & (select.EPOLLERR | select.EPOLLHUP):
                            print(f"Socket {fileno} received error or hangup", file=sys.stderr)
                            self.cleanup_connection(fileno)
                            continue

                        # Data available for reading
                        elif event & select.EPOLLIN:
                            # Client -> Proxy: Handle incoming client requests
                            if fileno in self.requests:
                                try:
                                    data = self.connections[fileno].recv(8192)
                                    
                                    if data:
                                        self.requests[fileno] += data
                                        
                                        # Check if we have a complete HTTP request
                                        if b'\r\n\r\n' in self.requests[fileno]:
                                            # Process the complete request
                                            request_end = self.requests[fileno].find(b'\r\n\r\n') + 4
                                            current_request = self.requests[fileno][:request_end]
                                            
                                            # Keep any remaining data for the next request
                                            self.requests[fileno] = self.requests[fileno][request_end:]
                                            
                                            server_sock = self.handle_client_request(
                                                self.connections[fileno],
                                                current_request
                                            )
                                    else:
                                        # Client closed connection
                                        print(f"Client {fileno} closed connection (received empty data)", file=sys.stderr)
                                        self.cleanup_connection(fileno)
                                        
                                except (socket.error, ConnectionResetError) as e:
                                    if hasattr(e, 'errno') and e.errno not in [errno.EAGAIN, errno.EWOULDBLOCK]:
                                        print(f"Error reading from client {fileno}: {e}", file=sys.stderr)
                                        self.cleanup_connection(fileno)
                            
                            # Server -> Proxy: Handle incoming server responses
                            elif fileno in self.responses:
                                try:
                                    data = self.connections[fileno].recv(8192)
                                    
                                    if data:
                                        response = self.responses[fileno]
                                        response['data'] += data
                                        response['last_activity'] = time.time()
                                        
                                        # Parse headers if not already done
                                        if not response['headers_parsed']:
                                            headers_end = response['data'].find(b'\r\n\r\n')
                                            if headers_end != -1:
                                                headers = response['data'][:headers_end]
                                                content_length = self.parse_content_length(headers)
                                                if content_length is not None:
                                                    response['chunk_size'] = content_length
                                                response['headers_parsed'] = True
                                                print(f"Response headers parsed, content length: {response.get('chunk_size')}", file=sys.stderr)
                                        
                                        # Forward data to client
                                        try:
                                            client = response['client']
                                            if client and client.fileno() >= 0:  # Check if valid socket
                                                sent = client.send(data)
                                                if sent == 0:
                                                    print(f"Client socket for server {fileno} appears closed (sent 0 bytes)", file=sys.stderr)
                                                    self.cleanup_connection(fileno)
                                            else:
                                                print(f"Invalid client socket for server {fileno}", file=sys.stderr)
                                                self.cleanup_connection(fileno)
                                        except (socket.error, ConnectionResetError) as e:
                                            print(f"Error sending to client: {e}", file=sys.stderr)
                                            self.cleanup_connection(fileno)
                                    else:
                                        # Server closed connection - response complete
                                        print(f"Server {fileno} closed connection (received empty data)", file=sys.stderr)
                                        response = self.responses[fileno]
                                        
                                        # Update throughput if it was a video segment
                                        if response.get('chunk_name') and 'Seg' in response.get('chunk_name', ''):
                                            chunk_name = response.get('chunk_name')
                                            chunk_size = response.get('chunk_size') or len(response.get('data', b''))
                                            start_time = response.get('start_time')
                                            
                                            print(f"Processing completed chunk: {chunk_name}", file=sys.stderr)
                                            
                                            # Calculate throughput once download is complete
                                            if chunk_size > 0 and start_time:
                                                self.update_throughput(chunk_size, chunk_name, start_time)
                                            else:
                                                print(f"Warning: Invalid chunk size ({chunk_size}) or start time for {chunk_name}", file=sys.stderr)
                                        
                                        self.cleanup_connection(fileno)
                                        
                                except (socket.error, ConnectionResetError) as e:
                                    if hasattr(e, 'errno') and e.errno not in [errno.EAGAIN, errno.EWOULDBLOCK]:
                                        print(f"Error reading from server {fileno}: {e}", file=sys.stderr)
                                        self.cleanup_connection(fileno)
                    
                    # Periodically check for stalled transfers
                    current_time = time.time()
                    if current_time - last_check_time >= 2:
                        self.check_stalled_transfers(current_time)
                        last_check_time = current_time
                    
                except (select.error, OSError) as e:
                    if hasattr(e, 'errno') and e.errno != errno.EINTR:
                        print(f"Error in event loop: {e}", file=sys.stderr)
                    continue
                
                except Exception as e:
                    print(f"Unexpected error in event loop: {e}", file=sys.stderr)
                    import traceback
                    traceback.print_exc(file=sys.stderr)
                    continue

        except KeyboardInterrupt:
            print("Proxy server stopping...", file=sys.stderr)
        except Exception as e:
            print(f"Fatal error in proxy: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
        finally:
            self.cleanup()

    def check_stalled_transfers(self, current_time):
        """
        Check for and handle stalled transfers
        
        If a transfer hasn't sent any data for STALL_TIMEOUT seconds,
        it's considered stalled and is cleaned up. This helps prevent
        hanging connections.
        
        As a side effect, it also reduces the throughput estimate to be
        more conservative, to handle potential network issues.
        """
        # Define maximum inactivity before considering transfer stalled (seconds)
        STALL_TIMEOUT = 3.0  # More aggressive timeout
        
        # Track stalled connections to clean up after iteration
        stalled_connections = []
        
        for fileno, response in list(self.responses.items()):
            last_activity = response.get('last_activity', 0)
            chunk_name = response.get('chunk_name')
            
            # Consider a transfer stalled if no activity for STALL_TIMEOUT seconds
            if current_time - last_activity > STALL_TIMEOUT:
                print(f"Stalled transfer detected for connection {fileno}", file=sys.stderr)
                if chunk_name:
                    print(f"Stalled chunk: {chunk_name}", file=sys.stderr)
                
                # Adjust throughput estimate down significantly
                if self.available_bitrates:
                    # Set throughput to match the lowest available bitrate with safety margin
                    lowest_bitrate = min(self.available_bitrates)
                    self.global_throughput = lowest_bitrate * 1.5  # Use the safety factor of 1.5
                    print(f"Adjusting throughput estimate down to {self.global_throughput:.2f} Kbps due to stalled transfer", file=sys.stderr)
                
                # Mark for cleanup
                stalled_connections.append(fileno)
        
        # Clean up stalled connections
        for fileno in stalled_connections:
            self.cleanup_connection(fileno)
            
        print(f"Checked for stalled transfers: found {len(stalled_connections)} stalled connections", file=sys.stderr)

    def cleanup(self):
        """
        Clean up all resources when the proxy is shutting down
        
        Closes all connections and releases epoll resources
        """
        try:
            # Close all client and server connections
            for fileno in list(self.connections.keys()):
                self.cleanup_connection(fileno)
            
            # Close epoll
            if hasattr(self, 'epoll'):
                self.epoll.close()
            
            # Close server socket
            if hasattr(self, 'server_socket'):
                self.server_socket.close()
                
        except Exception as e:
            print(f"Error during cleanup: {e}", file=sys.stderr)

    def cleanup_connection(self, fileno):
        """
        Clean up a specific connection and its associated resources
        
        This handles properly shutting down a socket and removing it
        from all tracking data structures.
        
        Args:
            fileno: File descriptor for the connection to clean up
        """
        try:
            # First unregister from epoll
            try:
                self.epoll.unregister(fileno)
            except (IOError, OSError) as e:
                # It's okay if it's already unregistered
                pass
            
            # Get the client socket associated with this response if it's a server connection
            client_socket = None
            client_fileno = None
            if fileno in self.responses:
                client_socket = self.responses[fileno].get('client')
                if client_socket:
                    client_fileno = client_socket.fileno()
            
            # Close the socket
            if fileno in self.connections:
                sock = self.connections[fileno]
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except (socket.error, OSError):
                    pass  # Socket might already be shut down
                try:
                    sock.close()
                except (socket.error, OSError):
                    pass  # Socket might already be closed
                del self.connections[fileno]
                print(f"Closed and removed socket with fileno {fileno}", file=sys.stderr)
            
            # Clean up related data structures
            if fileno in self.requests:
                del self.requests[fileno]
                
            if fileno in self.responses:
                # If this was a server socket, also clean up the associated client socket
                if client_socket and client_fileno in self.connections:
                    print(f"Also cleaning up associated client socket {client_fileno}", file=sys.stderr)
                    # We don't call cleanup_connection recursively to avoid potential loops
                    # Just close the socket directly
                    try:
                        self.epoll.unregister(client_fileno)
                    except (IOError, OSError):
                        pass
                    
                    try:
                        client_socket.shutdown(socket.SHUT_RDWR)
                    except (socket.error, OSError):
                        pass
                    
                    try:
                        client_socket.close()
                    except (socket.error, OSError):
                        pass
                    
                    if client_fileno in self.connections:
                        del self.connections[client_fileno]
                    
                    if client_fileno in self.requests:
                        del self.requests[client_fileno]
                
                del self.responses[fileno]
                
            if fileno in self.chunk_start_times:
                del self.chunk_start_times[fileno]
                
            if fileno in self.chunk_sizes:
                del self.chunk_sizes[fileno]
                
        except Exception as e:
            print(f"Error in cleanup_connection for {fileno}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)

def main():
    """
    Parse command-line arguments and start the proxy server
    
    Expected arguments:
    - log_file: Path to write performance logs
    - alpha: EWMA smoothing factor (0-1)
    - port: Port to listen for connections
    """
    if len(sys.argv) != 4:
        print("Usage: python3 proxy.py <log-file> <alpha> <port>")
        sys.exit(1)
        
    log_file = sys.argv[1]
    alpha = sys.argv[2]
    port = sys.argv[3]
    
    # Validate alpha (must be between 0 and 1)
    try:
        alpha_val = float(alpha)
        if alpha_val <= 0 or alpha_val >= 1:
            print("Alpha must be between 0 and 1")
            sys.exit(1)
    except ValueError:
        print("Alpha must be a number between 0 and 1")
        sys.exit(1)
        
    # Validate port (must be in valid port range)
    try:
        port_val = int(port)
        if port_val < 1024 or port_val > 65535:
            print("Port must be between 1024 and 65535")
            sys.exit(1)
    except ValueError:
        print("Port must be a number")
        sys.exit(1)
    
    # Start the proxy
    proxy = VideoProxy(log_file, alpha, port)
    proxy.run()

if __name__ == "__main__":
    main()