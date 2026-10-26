# link

A tiny, general-purpose bridge between **Google Colab** (where your model runs) and **your local machine** (where your GUI/app runs).

One file (`link.py`), one dependency (`websockets`), no framework. It gives you:

- Request/response calls with any JSON payload plus optional binary data
- Streaming in both directions (audio out, mic audio in, progress updates)
- File upload and download
- Cancellation, concurrent calls on one connection, and server-side errors that surface on the client
- A free public tunnel (Cloudflare quick tunnel) so Colab is reachable from your PC, with no account needed

The typical use: tweak TTS parameters in a local GUI, send them to Colab, and get audio streamed back. The library knows nothing about TTS, so it works for any model or workload.

```
 Local PC (GUI)  ── wss:// (Cloudflare tunnel) ──▶  Colab (model)
   Client                                              Server + handlers
```

## Install

```
pip install "websockets>=13"
```

Copy `link.py` next to your code on **both** machines, or clone the repo:

```python
# Colab
!git clone https://github.com/Tankist-edge-tech/link
%cd link
!pip -q install "websockets>=13"
```

## Quick start

**Colab**

```python
from link import Server
srv = Server()

@srv.handler("tts")
def tts(req):                                   # generator -> streamed reply
    for chunk in my_model.stream(req.meta["text"], pitch=req.meta["pitch"]):
        yield {"sr": 24000}, chunk.tobytes()

srv.start(tunnel=True)                          # prints URL and TOKEN
```

**Local PC**

```python
from link import Client
c = Client("wss://xxxx.trycloudflare.com", token="...")

for m in c.start("tts", {"text": "hello", "pitch": 1.1}):
    player.write(m.data)
```

Try everything without a model: `example_server.py` uses a fake tone generator.

```
python example_server.py --local                # terminal 1 (token: LINK_TOKEN env var)
python example_client.py ws://localhost:8765 <token>   # terminal 2
```

## Concepts

Everything is a `Message`:

| Field | Meaning |
|---|---|
| `kind` | Name of the handler to call |
| `meta` | Any JSON-able dict (your parameters, results, status) |
| `data` | Raw bytes (audio, files), never base64-encoded |

A **call** is one request and its replies. Many calls can run at the same time over one connection.

## Server API

Register handlers with `@srv.handler("name")`. The function type decides the behaviour:

| Handler type | Behaviour |
|---|---|
| `def f(req)` | One reply. Runs in a thread, so blocking model code is fine |
| `def f(req)` with `yield` | Streams replies. Runs in a thread |
| `async def f(req)` | One reply, runs on the event loop |
| `async def f(req)` with `yield` | Streams replies, and can read incoming streams |

Return or yield any of: `None`, `bytes`, a `dict` (meta), or `(meta, bytes)`.

`req` has `.meta`, `.data`, `.kind`, and `async for m in req.incoming()` for extra messages the client sends on the same call (async handlers only).

```python
srv = Server(port=8765, token=None)   # random token if None
srv.start(tunnel=True)                # background thread; tunnel=False for local
srv.join()                            # block forever (plain scripts)
srv.stop()
```

## Client API

```python
c = Client(url, token)

c.call(kind, meta, data, timeout)     # send, block for the single reply -> Message
call = c.start(kind, meta, data)      # begin a call; iterate it for streamed replies
for m in call: ...
call.send(meta, data)                 # send more messages on an open call
call.finish()                         # close the request side
call.cancel()                         # stop the server handler
c.close()
```

For a call where you stream data in, use `c.start(kind, end=False)` and finish it with `call.finish()`.

Errors: handler exceptions raise `RemoteError`, a dropped connection raises `ConnectionError`, and a missed `timeout` raises `TimeoutError`.

## Use it from a GUI

Never block the UI thread. Start a worker thread that iterates the call and pushes results into a `queue.Queue`, and poll the queue from the UI. See `example_gui.py` (Tkinter; the same pattern works for Qt and others).

## Examples

| File | Shows |
|---|---|
| `example_server.py` | One handler per pattern, with a fake model |
| `example_client.py` | Simple call, file result, audio streaming, file upload and download, mic-style streaming in, cancel, concurrency, config, errors |
| `example_gui.py` | Sliders and Generate/Stop buttons with streamed audio playback |

## Security

The tunnel URL is public. The only protection is the random token checked on connect, so keep the token private and don't commit it. Connections are encrypted (`wss://`) through the tunnel. Anything you register as a handler is callable by whoever holds the token, so don't expose handlers that run arbitrary code or read arbitrary paths. Validate file names, as `example_server.py` does with `Path(name).name`.

## Limitations

- No auto-reconnect: if Colab restarts, start the server again and use the new URL and token.
- No server-initiated pushes outside of a call.
- Cancelling a sync generator takes effect after its current `yield` returns.
- Cloudflare quick tunnels are free and best-effort, with no uptime guarantee.

## License

MIT
