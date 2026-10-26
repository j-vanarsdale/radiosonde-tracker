#!/usr/bin/env python3
"""
radiosonde.py - receive radiosonde_auto_rx PAYLOAD_SUMMARY UDP packets,
serve the live targets as JSON, and log every packet to a JSONL file.

  UDP  :<udp_port>            <- auto_rx (Pi 2)
  HTTP 127.0.0.1:<api_port>/sondes -> [{callsign, latitude, longitude, ...}]
"""

import argparse
import json
import socket
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sondes = {}          # callsign -> latest packet + last_seen
lock = threading.Lock()


def listen(udp_port, log):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", udp_port))
    while True:
        data, _ = sock.recvfrom(65535)
        try:
            pkt = json.loads(data)
        except ValueError:
            continue
        if not isinstance(pkt, dict) or pkt.get("type") != "PAYLOAD_SUMMARY" or not pkt.get("callsign"):
            continue
        now = time.time()
        rx_time = datetime.fromtimestamp(now, timezone.utc).isoformat()
        log.write(json.dumps({"rx_time": rx_time, **pkt}) + "\n")
        log.flush()
        with lock:
            sondes[pkt["callsign"]] = {**pkt, "last_seen": now}


def make_handler(timeout):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.split("?")[0] != "/sondes":
                self.send_error(404)
                return
            now = time.time()
            with lock:
                for cs in [c for c, s in sondes.items() if now - s["last_seen"] > timeout]:
                    del sondes[cs]
                items = [{**s, "age_s": now - s["last_seen"]} for s in sondes.values()]
            body = json.dumps(sorted(items, key=lambda s: s["callsign"])).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


def main():
    p = argparse.ArgumentParser(description="Radiosonde target feed for Eventide")
    p.add_argument("--udp_port", type=int, default=55673, help="auto_rx payload_summary_port")
    p.add_argument("--api_port", type=int, required=True, help="HTTP port for the dashboard")
    p.add_argument("--output-dir", required=True, help="Directory for the JSONL log")
    p.add_argument("--timeout", type=float, default=300, help="Drop a sonde after this many seconds of silence")
    args = p.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    log = open(out / f"radiosonde_{datetime.now():%Y%m%d_%H%M%S}.jsonl", "a")

    threading.Thread(target=listen, args=(args.udp_port, log), daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", args.api_port), make_handler(args.timeout)).serve_forever()


if __name__ == "__main__":
    main()
