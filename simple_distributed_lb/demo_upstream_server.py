import json
import logging
import os
import sys
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer

# A dictionary to keep count of the requests
request_counts = defaultdict(int)

log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
log_level = logging.getLevelName(log_level)
logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger()


class RequestHandler(BaseHTTPRequestHandler):
    def inc_counter(self, method, path):
        global request_counts

        request_counts[f"{self.command}|{self.path}"] += 1

    def do_GET(self):
        self.inc_counter(self.command, self.path)

        # Check if the path is /stats to respond with the statistics
        if self.path == "/stats":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            # Send the request counts as JSON
            self.wfile.write(json.dumps(request_counts, default=str).encode())
        else:
            # Respond with a simple message for other GET requests
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Hello! This is a simple HTTP server.")

    def do_POST(self):
        self._handle_request()

    def do_PUT(self):
        self._handle_request()

    def do_DELETE(self):
        self._handle_request()

    def _handle_request(self):
        self.inc_counter(self.command, self.path)

        # Respond with a simple message
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Request received and counted.")

    def format_log_message(self, format, *args):
        message = format % args
        safe_message = message.translate(self._control_char_table)
        formatted = f"{self.address_string()} - - [{self.log_date_time_string()}] {safe_message}\n"
        return formatted

    def log_message(self, format, *args):
        msg = self.format_log_message(format, *args)
        logger.info(msg)

    def log_error(self, format, *args):
        msg = self.format_log_message(format, *args)
        logger.error(msg)


def run(server_class=HTTPServer, handler_class=RequestHandler, port=8080):
    port = int(os.environ.get("APP_PORT", port))
    server_address = ("", port)
    httpd = server_class(server_address, handler_class)
    logger.info(f"Starting httpd server on port {port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopping httpd server")


if __name__ == "__main__":
    run()
