# HTTP Proxy for Adaptive Video Streaming

## Description
This project implements an HTTP proxy that facilitates adaptive video streaming between web browsers and a DASH (Dynamic Adaptive Streaming over HTTP) server. The proxy intercepts HTTP requests from the browser, modifies them to optimize video playback quality based on estimated network conditions, and logs detailed activity for analysis.

Key features:
- **Adaptive Bitrate Selection**: Selects the highest bitrate that can be supported by the current throughput estimate (with a safety factor of 1.5)
- **Manifest File Handling**: Intercepts manifest.mpd requests and redirects to manifest_nolist.mpd while parsing the original to get available bitrates
- **Throughput Estimation**: Uses Exponentially Weighted Moving Average (EWMA) formula to estimate network throughput
- **Concurrent Connection Handling**: Implements epoll() to efficiently handle multiple client connections
- **Request/Response Pipelining**: Properly handles HTTP request/response pipelining

## Implementation Details

### Throughput Estimation
The proxy calculates throughput for each video chunk downloaded using:
```
Throughput (Kbps) = Chunk Size (bits) / Download Time (seconds)
```

It then applies the EWMA formula with the specified alpha value:
```
T_current = α * T_new + (1 - α) * T_current
```

### Bitrate Selection
Following the project requirements, the proxy selects the highest bitrate for which the current throughput estimate is at least 1.5 times the bitrate. The available bitrates are extracted from the manifest.mpd file.

### Assumptions
1. The DASH server is located at IP 149.165.170.233 on port 80
2. The client is requesting DASH video segments in the format [bitrate]Seg[chunkNum]
3. All requests follow standard HTTP/1.1 protocol
4. The manifest.mpd file contains properly formatted Representation elements with bandwidth attributes

## Instructions to Run

1. Ensure Python 3 is installed on your system

2. Start the proxy with:
```
python3 proxy.py <log-file> <alpha> <port>
```
Where:
- `<log-file>`: Path to the output log file (e.g., proxy.log)
- `<alpha>`: Smoothing factor for EWMA (between 0 and 1)
- `<port>`: Port to listen for incoming connections

3. Configure your web browser to use the proxy by setting its HTTP proxy to localhost and the port you specified

4. Open your browser and navigate to:
```
http://localhost:<port>/index.html
```

5. To test different network conditions, you can use the provided bandwidth throttling script:
```
./bandwidth_throttle.sh set 500kbit
```

6. After collecting data, you can visualize the bitrate adaptation using the provided grapher.py script:
```
python3 grapher.py
```

## Notes
- I find that my proxy only streams video consistently at a bitrate of 10 or 1000. I tried many different caps on the ingress values but none of them were able to produce another bitrate.
- The `clean` flag for the `bandwidth_throttle.sh` doesn't work, so I had to always manually reset the port's bandwidth by running something like `./bandwidth_throttle.sh set 1000kbit 80`.