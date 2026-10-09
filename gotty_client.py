#!/usr/bin/env python3
"""WebSocket terminal client for GoTTY (Unix only).

GoTTY protocol (text frames)
  Client -> Server: '1' Input, '2' Ping, '3' ResizeTerminal
  Server -> Client: '1' Output (base64), '2' Pong, '3' SetWindowTitle,
                    '4' SetPreferences, '5' SetReconnect
  The first client message is plain JSON: {"AuthToken": "..."}

Exit the local client with Ctrl+]  (Ctrl+C / Ctrl+D go to the remote shell).
"""
import argparse
import asyncio
import base64
import binascii
import codecs
import json
import os
import re
import signal
import sys
import termios
import tty
import urllib.parse

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI

GOTTY_URL = "ws://localhost:8080/ws"
EXIT_KEY = b"\x1d"  # Ctrl+]

DEBUG = False


class AuthError(Exception):
    """Authentication failed (not retried)."""


def debug_print(*args):
    """Debug output. '\\r\\n' is used because the terminal is in raw mode."""
    if DEBUG:
        sys.stderr.write(" ".join(str(a) for a in args) + "\r\n")
        sys.stderr.flush()


def parse_args():
    parser = argparse.ArgumentParser(description="WebSocket terminal client for GoTTY")
    parser.add_argument("--url", default=GOTTY_URL,
                        help="WebSocket URL (default: %(default)s)")
    parser.add_argument("--retries", type=int, default=3,
                        help="Max connection attempts with exponential backoff (default: %(default)s)")
    parser.add_argument("--username", help="Username for basic authentication")
    parser.add_argument("--password", help="Password for basic authentication")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    return parser.parse_args()


def basic_auth_headers(username, password):
    if username and password:
        raw = f"{username}:{password}".encode("utf-8")
        return {"Authorization": "Basic " + base64.b64encode(raw).decode("ascii")}
    return {}


def ws_extra_headers_kwargs(headers):
    """websockets >= 14 uses additional_headers, older versions extra_headers."""
    if not headers:
        return {}
    try:
        major = int(websockets.__version__.split(".")[0])
    except ValueError:
        major = 14
    key = "additional_headers" if major >= 14 else "extra_headers"
    return {key: headers}


async def fetch_auth_token(ws_url, headers, has_credentials):
    """Fetch the token from <prefix>/auth_token.js. Returns None if not needed."""
    p = urllib.parse.urlparse(ws_url)
    scheme = "https" if p.scheme == "wss" else "http"
    path = urllib.parse.urljoin(p.path or "/", "auth_token.js")
    url = urllib.parse.urlunparse((scheme, p.netloc, path, "", "", ""))
    debug_print(f"Fetching auth token from: {url}")

    timeout = aiohttp.ClientTimeout(total=5)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 401:
                    raise AuthError("HTTP 401 Unauthorized - check credentials")
                if resp.status == 404 and not has_credentials:
                    debug_print("auth_token.js not found - assuming no auth")
                    return None
                resp.raise_for_status()
                body = await resp.text()
    except aiohttp.ClientError as e:
        if not has_credentials:
            debug_print(f"auth_token.js unavailable ({e}) - assuming no auth")
            return None
        raise ConnectionError(f"Failed to fetch auth token: {e}")

    m = re.search(r"var gotty_auth_token = '([^']*)'", body)
    token = m.group(1) if m else None
    debug_print("Auth token fetched" if token else "No auth token in response")
    return token or None


async def main_session(ws, auth_token):
    fd = sys.stdin.fileno()
    loop = asyncio.get_running_loop()
    old_settings = termios.tcgetattr(fd)

    stdin_queue = asyncio.Queue()
    resize_event = asyncio.Event()
    # Stdin chunks can split a multibyte character; decode incrementally.
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    tasks = []

    async def send_message(msg_type, data=""):
        await ws.send(msg_type + data)

    def on_stdin_readable():
        try:
            data = os.read(fd, 1024)
        except OSError:
            data = b""
        stdin_queue.put_nowait(data)
        if not data:
            loop.remove_reader(fd)

    async def send_input():
        while True:
            data = await stdin_queue.get()
            if not data:
                debug_print("EOF on stdin")
                return
            if EXIT_KEY in data:
                debug_print("Exit key (Ctrl+]) pressed")
                return
            text = decoder.decode(data)
            if text:
                await send_message("1", text)

    async def send_ping():
        while True:
            await asyncio.sleep(10)
            await send_message("2")

    async def send_size():
        try:
            cols, rows = os.get_terminal_size(fd)
        except OSError as e:
            debug_print(f"Failed to get terminal size: {e}")
            return
        await send_message("3", json.dumps({"columns": cols, "rows": rows}))
        debug_print(f"-> Resize: {cols}x{rows}")

    async def resize_watcher():
        while True:
            await resize_event.wait()
            resize_event.clear()
            await send_size()

    async def recv_output():
        async for msg in ws:
            if isinstance(msg, str):
                msg = msg.encode("utf-8")
            if not msg:
                continue
            msg_type, msg_data = msg[:1], msg[1:]

            if msg_type == b"1":  # Output (base64)
                try:
                    out = base64.b64decode(msg_data)
                except binascii.Error as e:
                    debug_print(f"Failed to decode base64 output: {e}")
                    continue
                sys.stdout.buffer.write(out)
                sys.stdout.buffer.flush()
            elif msg_type == b"2":  # Pong
                debug_print("<- Pong")
            elif msg_type == b"3":  # SetWindowTitle (plain text)
                title = msg_data.decode("utf-8", errors="replace")
                sys.stdout.write(f"\033]0;{title}\007")
                sys.stdout.flush()
            elif msg_type == b"4":  # SetPreferences
                debug_print(f"<- SetPreferences: {msg_data[:200]!r}")
            elif msg_type == b"5":  # SetReconnect
                debug_print(f"<- SetReconnect: {msg_data[:50]!r}")
            else:
                debug_print(f"<- Unknown message type: {msg_type!r}")

    try:
        tty.setraw(fd)
        loop.add_reader(fd, on_stdin_readable)
        loop.add_signal_handler(signal.SIGWINCH, resize_event.set)

        # First message: plain JSON, no type prefix
        await ws.send(json.dumps({"AuthToken": auth_token} if auth_token else {}))
        await send_size()

        tasks = [asyncio.create_task(c) for c in
                 (send_input(), send_ping(), recv_output(), resize_watcher())]
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            t.result()  # re-raise ConnectionClosed etc. so the caller can retry
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        loop.remove_reader(fd)
        loop.remove_signal_handler(signal.SIGWINCH)
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        print("\n🔌 Disconnected. Terminal restored.", file=sys.stderr)


async def connect_with_retry(uri, max_retries, username=None, password=None):
    headers = basic_auth_headers(username, password)
    has_credentials = bool(headers)
    max_retries = max(1, max_retries)

    for attempt in range(max_retries):
        try:
            # Re-fetch every time: tokens may expire between attempts
            token = await fetch_auth_token(uri, headers, has_credentials)
            async with websockets.connect(
                uri, ping_interval=20, ping_timeout=10,
                **ws_extra_headers_kwargs(headers),
            ) as ws:
                print(f"✅ Connected to {uri}  (exit: Ctrl+])", file=sys.stderr)
                await main_session(ws, token)
                return
        except (AuthError, InvalidURI):
            raise  # retrying will not help
        except (OSError, InvalidHandshake, ConnectionClosed, asyncio.TimeoutError) as e:
            msg = "Session disconnected" if isinstance(e, ConnectionClosed) else str(e)
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"\n⚠️  Attempt {attempt + 1}/{max_retries} failed: {msg}",
                      file=sys.stderr)
                print(f"   Retrying in {wait}s...", file=sys.stderr)
                await asyncio.sleep(wait)
            else:
                print(f"\n❌ Failed after {max_retries} attempts.", file=sys.stderr)
                raise


def main():
    global DEBUG
    args = parse_args()
    DEBUG = args.debug

    if not sys.stdin.isatty():
        print("❌ stdin is not a TTY.", file=sys.stderr)
        sys.exit(1)

    try:
        asyncio.run(connect_with_retry(args.url, args.retries,
                                       args.username, args.password))
    except KeyboardInterrupt:
        print("\n⛔ Interrupted by user.", file=sys.stderr)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
