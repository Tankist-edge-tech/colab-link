"""example_server.py - COLAB side. One handler per pattern you'll need.

The "model" here is a fake tone generator so the demo runs anywhere.
Replace FakeTTS with your real model; the handlers barely change.

Colab:
    !pip -q install "websockets>=13"
    (upload link.py and this file)
    %run example_server.py          # prints URL + TOKEN, paste them on your PC
Local test:
    LINK_TOKEN=dev python example_server.py --local
"""
import array, hashlib, io, math, os, sys, time, wave
from pathlib import Path

from link import Server

SR = 24000
INBOX = Path("inbox"); INBOX.mkdir(exist_ok=True)      # where uploaded files land
CONFIG = {"voice": "default"}                          # server-side state shared by all calls


# ------------------------------------------------------------------ fake model
class FakeTTS:
    """Stand-in for your real model. Yields int16 PCM bytes, ~80 ms per character."""
    def stream(self, text, pitch=1.0, speed=1.0, volume=0.3, **_):
        for ch in text:
            freq = (200 + (ord(ch) % 24) * 20) * pitch
            n = int(SR * 0.08 / speed)
            amp = int(32767 * volume)
            buf = array.array("h", (int(amp * math.sin(2 * math.pi * freq * i / SR))
                                    for i in range(n)))
            time.sleep(0.05)                           # pretend the GPU is working
            yield buf.tobytes()

    def generate(self, text, **kw):
        return b"".join(self.stream(text, **kw))


model = FakeTTS()


def to_wav(pcm: bytes, sr=SR) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1); w.setsampwidth(2); w.setframerate(sr)
        w.writeframes(pcm)
    return buf.getvalue()


srv = Server(token=os.environ.get("LINK_TOKEN"))      # random token if env var not set


# 1) simplest: plain function, meta in -> meta out ------------------------------------
@srv.handler("ping")
def ping(req):
    return {"pong": True, "echo": req.meta, "server_time": time.time()}


# 2) request params -> one binary result (a whole file) -------------------------------
@srv.handler("tts_file")
def tts_file(req):
    pcm = model.generate(**req.meta)                   # meta is just your params dict
    return {"sr": SR, "format": "wav"}, to_wav(pcm)    # (meta, bytes)


# 3) request params -> STREAM of audio chunks (generator = streaming) -----------------
@srv.handler("tts_stream")
def tts_stream(req):
    for i, chunk in enumerate(model.stream(**req.meta)):
        yield {"sr": SR, "i": i}, chunk                # each yield = one message


# 4) client sends a file, server stores it ---------------------------------------------
@srv.handler("upload")
def upload(req):
    name = Path(req.meta["name"]).name                 # .name strips any ../ tricks
    (INBOX / name).write_bytes(req.data)
    return {"saved": name, "size": len(req.data),
            "sha256": hashlib.sha256(req.data).hexdigest()}


# 5) client downloads a file from the server -------------------------------------------
@srv.handler("download")
def download(req):
    path = INBOX / Path(req.meta["name"]).name
    return {"name": path.name}, path.read_bytes()


# 6) client streams data IN (mic -> STT) and gets partial results OUT -------------------
#    Inbound streaming needs `async def` + req.incoming().
@srv.handler("transcribe")
async def transcribe(req):
    total = 0
    async for chunk in req.incoming():
        total += len(chunk.data)
        yield {"partial": f"heard {total} bytes so far"}
    yield {"final": f"done, {total} bytes total"}


# 7) long-running job with progress; the client can cancel it ---------------------------
@srv.handler("count")
def count(req):
    for i in range(req.meta.get("n", 10)):
        time.sleep(0.2)
        yield {"progress": i + 1, "of": req.meta.get("n", 10)}


# 8) server-side state / settings ------------------------------------------------------
@srv.handler("set_config")
def set_config(req):
    CONFIG.update(req.meta)
    return CONFIG


# 9) errors: exceptions travel back and raise RemoteError on the client ----------------
@srv.handler("boom")
def boom(req):
    raise ValueError("this is what a server-side crash looks like")


if __name__ == "__main__":
    local = "--local" in sys.argv
    srv.start(tunnel=not local)                        # tunnel=True on Colab
    if local:
        srv.join()                                     # plain script: keep process alive
    # in a Colab cell the background thread keeps running after the cell finishes
