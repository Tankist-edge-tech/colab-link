"""link.py - tiny RPC + streaming over one WebSocket. Use the same file on Colab and PC.
    pip install "websockets>=13"
"""
import asyncio, inspect, itertools, json, os, queue, re, secrets, shutil
import struct, subprocess, threading, traceback, urllib.request
from dataclasses import dataclass, field

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

# ---------------------------------------------------------------- wire format
@dataclass
class Message:
    kind: str = ""
    meta: dict = field(default_factory=dict)
    data: bytes = b""
    id: int = 0
    end: bool = True          # last message of this call (in this direction)

    @property
    def is_empty(self):
        return not self.meta and not self.data


def pack(m: Message) -> bytes:
    head = json.dumps({"k": m.kind, "i": m.id, "e": m.end, "m": m.meta}).encode()
    return struct.pack(">I", len(head)) + head + m.data


def unpack(raw: bytes) -> Message:
    n = struct.unpack(">I", raw[:4])[0]
    h = json.loads(raw[4:4 + n])
    return Message(h["k"], h["m"], bytes(raw[4 + n:]), h["i"], h["e"])


def _norm(x):
    """Handler output -> (meta, data). Accepts None, bytes, dict, or (meta, bytes)."""
    if x is None:
        return {}, b""
    if isinstance(x, (bytes, bytearray)):
        return {}, bytes(x)
    if isinstance(x, dict):
        return x, b""
    meta, data = x
    return meta or {}, data or b""


class RemoteError(Exception):
    pass


# ---------------------------------------------------------------------- server
class Request:
    """Passed to handlers. First message is on .meta/.data; more via .incoming()."""
    def __init__(self, m: Message):
        self.id, self.kind, self.meta, self.data = m.id, m.kind, m.meta, m.data
        self._q = asyncio.Queue()
        if m.end:
            self._q.put_nowait(None)

    def _feed(self, m: Message):
        if not m.is_empty:
            self._q.put_nowait(m)
        if m.end:
            self._q.put_nowait(None)

    async def incoming(self):
        """Async-iterate further messages the client sends on this call (e.g. mic audio)."""
        while (m := await self._q.get()) is not None:
            yield m


_DONE = object()


class Server:
    def __init__(self, port=8765, host="0.0.0.0", token=None):
        self.port, self.host = port, host
        self.token = token or secrets.token_urlsafe(12)
        self.handlers = {}
        self.url = None

    def handler(self, kind):
        """Decorator: register a handler for messages of this kind."""
        def deco(fn):
            self.handlers[kind] = fn
            return fn
        return deco

    async def _run(self, req, send):
        fn = self.handlers.get(req.kind)
        reply = lambda out, end: send(req.id, req.kind, *_norm(out), end)
        try:
            if fn is None:
                raise LookupError(f"no handler for {req.kind!r}")
            if inspect.isasyncgenfunction(fn):
                async for out in fn(req):
                    await reply(out, False)
                await reply(None, True)
            elif inspect.isgeneratorfunction(fn):
                gen = fn(req)
                while (out := await asyncio.to_thread(next, gen, _DONE)) is not _DONE:
                    await reply(out, False)
                await reply(None, True)
            elif inspect.iscoroutinefunction(fn):
                await reply(await fn(req), True)
            else:
                await reply(await asyncio.to_thread(fn, req), True)
        except asyncio.CancelledError:
            await reply(None, True)
            raise
        except Exception as e:
            traceback.print_exc()
            await send(req.id, "error", {"error": f"{type(e).__name__}: {e}"}, b"", True)

    async def _conn(self, ws):
        hello = unpack(await ws.recv())
        if hello.kind != "hello" or hello.meta.get("token") != self.token:
            await ws.close(1008, "bad token")
            return
        await ws.send(pack(Message("hello")))

        async def send(id, kind, meta, data, end):
            try:
                await ws.send(pack(Message(kind, meta, data, id, end)))
            except ConnectionClosed:
                pass

        active = {}  # id -> (Request, Task)
        try:
            async for raw in ws:
                m = unpack(raw)
                if m.kind == "_cancel":
                    if m.id in active:
                        active[m.id][1].cancel()
                elif m.id in active:
                    active[m.id][0]._feed(m)
                else:
                    req = Request(m)
                    task = asyncio.create_task(self._run(req, send))
                    active[m.id] = (req, task)
                    task.add_done_callback(lambda _, i=m.id: active.pop(i, None))
        finally:
            for _, t in list(active.values()):
                t.cancel()

    async def _main(self, ready):
        self._stop = asyncio.Event()
        async with serve(self._conn, self.host, self.port, max_size=None):
            ready.set()
            await self._stop.wait()

    def start(self, tunnel=False):
        """Run in a background thread (works in notebooks). tunnel=True -> public wss:// URL."""
        ready = threading.Event()

        def run():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._main(ready))

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        ready.wait()
        self.url = open_tunnel(self.port) if tunnel else f"ws://localhost:{self.port}"
        print(f"URL:   {self.url}\nTOKEN: {self.token}")
        return self

    def join(self):          # for plain scripts: block forever
        self._thread.join()

    def stop(self):
        self._loop.call_soon_threadsafe(self._stop.set)


def open_tunnel(port) -> str:
    """Free Cloudflare quick tunnel, no account needed. Returns a wss:// URL."""
    exe = shutil.which("cloudflared")
    if not exe:
        exe = os.path.abspath("cloudflared")
        if not os.path.exists(exe):
            urllib.request.urlretrieve(
                "https://github.com/cloudflare/cloudflared/releases/latest/download/"
                "cloudflared-linux-amd64", exe)
            os.chmod(exe, 0o755)
    p = subprocess.Popen([exe, "tunnel", "--url", f"http://localhost:{port}"],
                         stderr=subprocess.PIPE, text=True)
    for line in p.stderr:
        if m := re.search(r"https://[-\w]+\.trycloudflare\.com", line):
            def drain():
                for _ in p.stderr:
                    pass
            threading.Thread(target=drain, daemon=True).start()
            return m.group(0).replace("https://", "wss://")
    raise RuntimeError("cloudflared exited without giving a URL")


# ---------------------------------------------------------------------- client
class Call:
    """One request/response (or stream). Iterate it to receive messages."""
    def __init__(self, client, kind, id, timeout):
        self._c, self.kind, self.id, self.timeout = client, kind, id, timeout
        self._q = queue.Queue()

    def send(self, meta=None, data=b"", end=False):
        """Send more messages on this call (e.g. live mic audio)."""
        self._c._post(Message(self.kind, meta or {}, data, self.id, end))

    def finish(self):
        self.send(end=True)

    def cancel(self):
        self._c._post(Message("_cancel", id=self.id))

    def __iter__(self):
        while True:
            try:
                m = self._q.get(timeout=self.timeout)
            except queue.Empty:
                raise TimeoutError(f"no reply for {self.kind!r}") from None
            if isinstance(m, Exception):
                raise m
            if not (m.end and m.is_empty):      # empty end-message is just a terminator
                yield m
            if m.end:
                return

    def result(self) -> Message:
        """Wait for a single-reply call and return its (last) message."""
        last = Message()
        for last in self:
            pass
        return last


class Client:
    def __init__(self, url, token, connect_timeout=30):
        self._calls, self._ids = {}, itertools.count(1)
        self._loop = asyncio.new_event_loop()
        self._ready, self._err = threading.Event(), None
        threading.Thread(target=self._run, args=(url, token), daemon=True).start()
        if not self._ready.wait(connect_timeout):
            raise TimeoutError("could not connect")
        if self._err:
            raise ConnectionError(f"connection failed: {self._err!r}")

    def _run(self, url, token):
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._main(url, token))

    async def _main(self, url, token):
        try:
            async with connect(url, max_size=None) as ws:
                self._ws = ws
                await ws.send(pack(Message("hello", {"token": token})))
                unpack(await ws.recv())         # ack (raises if token rejected)
                self._ready.set()
                async for raw in ws:
                    m = unpack(raw)
                    call = self._calls.get(m.id)
                    if call is None:
                        continue
                    if m.kind == "error":
                        call._q.put(RemoteError(m.meta.get("error")))
                        self._calls.pop(m.id, None)
                    else:
                        call._q.put(m)
                        if m.end:
                            self._calls.pop(m.id, None)
        except Exception as e:
            self._err = e
        finally:
            self._ready.set()
            for c in list(self._calls.values()):
                c._q.put(ConnectionError("connection closed"))
            self._calls.clear()

    def _post(self, m: Message):
        asyncio.run_coroutine_threadsafe(self._ws.send(pack(m)), self._loop).result()

    def start(self, kind, meta=None, data=b"", end=True, timeout=None) -> Call:
        """Begin a call. Iterate the result for streamed replies. end=False keeps the
        request side open so you can call .send() / .finish() on it."""
        call = Call(self, kind, next(self._ids), timeout)
        self._calls[call.id] = call
        call.send(meta, data, end)
        return call

    def call(self, kind, meta=None, data=b"", timeout=None) -> Message:
        """Send a request, block until the single reply arrives."""
        return self.start(kind, meta, data, timeout=timeout).result()

    def close(self):
        asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
