#!/usr/bin/env python3
"""
radiosonde.py - receive radiosonde_auto_rx PAYLOAD_SUMMARY UDP packets,
serve the live targets as JSON, log every packet to JSONL, and point the
gimbal at a selected sonde.

  UDP  :<udp_port>                      <- auto_rx (Pi 2)
  GET  /sondes                          -> all live sondes (dashboard table)
  PUT  /sondes/track {"callsign": ...}  -> select a sonde and start gimbal URL-track
  DELETE /sondes/track                  -> stop tracking
  GET  /track                           -> selected sonde as {lat, lon, alt, ...}
                                           (polled by the gimbal's /track/url)
"""

import argparse
import json
import socket
import threading
import time
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sondes = {}          # callsign -> latest packet + last_seen
selected = None      # callsign currently being tracked
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


def make_handler(timeout, api_port, gimbal_url):
    def live():
        """Drop silent sondes, return the rest (call with lock held)."""
        now = time.time()
        for cs in [c for c, s in sondes.items() if now - s["last_seen"] > timeout]:
            del sondes[cs]
        return sondes

    def gimbal(method, path, body=None):
        """Call the gimbal API; return an error string or None."""
        req = urllib.request.Request(
            gimbal_url + path, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=5).read()
            return None
        except Exception as e:
            return f"gimbal API: {e}"

    class Handler(BaseHTTPRequestHandler):
        def reply(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?")[0]
            now = time.time()
            if path == "/sondes":
                with lock:
                    items = [{**s, "age_s": now - s["last_seen"]} for s in live().values()]
                self.reply(200, sorted(items, key=lambda s: s["callsign"]))
            elif path == "/track":
                with lock:
                    s = live().get(selected)
                if not s:
                    self.reply(404, {"error": "no tracked sonde"})
                    return
                self.reply(200, {
                    "lat": s.get("latitude"), "lon": s.get("longitude"), "alt": s.get("altitude"),
                    "speed": s.get("vel_h", s.get("speed")),
                    "heading": s.get("heading"),
                    "vertical_rate": s.get("vel_v"),
                })
            else:
                self.send_error(404)

        def do_PUT(self):
            global selected
            if self.path != "/sondes/track":
                self.send_error(404)
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
                callsign = json.loads(self.rfile.read(length)).get("callsign")
            except (ValueError, AttributeError):
                self.reply(400, {"error": "expected JSON {\"callsign\": ...}"})
                return
            with lock:
                if callsign not in live():
                    self.reply(404, {"error": f"unknown sonde: {callsign}"})
                    return
                selected = callsign
            err = gimbal("PUT", "/track/url", {
                "url": f"http://127.0.0.1:{api_port}/track",
                "interval": 1.0, "label": callsign})
            if err:
                with lock:
                    selected = None
                self.reply(502, {"error": err})
                return
            self.reply(200, {"status": "ok", "tracking": callsign})

        def do_DELETE(self):
            global selected
            if self.path != "/sondes/track":
                self.send_error(404)
                return
            with lock:
                selected = None
            err = gimbal("DELETE", "/track/url")
            self.reply(502 if err else 200, {"error": err} if err else {"status": "ok"})

        def log_message(self, *args):
            pass

    return Handler


def main():
    p = argparse.ArgumentParser(description="Radiosonde target feed for Eventide")
    p.add_argument("--udp_port", type=int, default=55673, help="auto_rx payload_summary_port")
    p.add_argument("--api_port", type=int, required=True, help="HTTP port for the dashboard")
    p.add_argument("--output-dir", required=True, help="Directory for the JSONL log")
    p.add_argument("--timeout", type=float, default=300, help="Drop a sonde after this many seconds of silence")
    p.add_argument("--gimbal_url", default="http://127.0.0.1:5001", help="Gimbal API base URL")
    args = p.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    log = open(out / f"radiosonde_{datetime.now():%Y%m%d_%H%M%S}.jsonl", "a")

    threading.Thread(target=listen, args=(args.udp_port, log), daemon=True).start()
    handler = make_handler(args.timeout, args.api_port, args.gimbal_url.rstrip("/"))
    ThreadingHTTPServer(("127.0.0.1", args.api_port), handler).serve_forever()


if __name__ == "__main__":
    main()
