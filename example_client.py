"""example_client.py - LOCAL PC side. Walks through every pattern, one function each.

    python example_client.py wss://xxxx.trycloudflare.com  <TOKEN>
    python example_client.py ws://localhost:8765 dev          # local test
"""
import sys, threading, time, wave
from dataclasses import dataclass, asdict
from pathlib import Path

from link import Client, RemoteError

OUT = Path("out"); OUT.mkdir(exist_ok=True)


# Custom payloads: meta is any JSON-able dict, so a dataclass + asdict() is all you need.
@dataclass
class TTSParams:
    text: str
    pitch: float = 1.0
    speed: float = 1.0
    volume: float = 0.3


def save_wav(path, pcm, sr=24000):
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1); w.setsampwidth(2); w.setframerate(sr)
        w.writeframes(pcm)


# 1 ---------------------------------------------------------------- simple call
def demo_ping(c):
    r = c.call("ping", {"hello": "world", "nested": {"a": [1, 2, 3]}})
    print("ping ->", r.meta)


# 2 --------------------------------------------- params in, whole file back
def demo_tts_file(c):
    r = c.call("tts_file", asdict(TTSParams("hello world", pitch=1.3, speed=0.9)))
    (OUT / "file.wav").write_bytes(r.data)
    print(f"tts_file -> {len(r.data)} bytes, sr={r.meta['sr']}")


# 3 ------------------------------------------------ streamed audio back
def demo_tts_stream(c):
    pcm, t0, first = bytearray(), time.time(), None
    for m in c.start("tts_stream", asdict(TTSParams("streaming audio demo"))):
        if first is None:
            first = time.time() - t0
        pcm += m.data
        # live playback would go here, e.g. with sounddevice:
        #   stream.write(np.frombuffer(m.data, dtype=np.int16))
    save_wav(OUT / "stream.wav", pcm)
    print(f"tts_stream -> first chunk after {first:.2f}s, total {time.time()-t0:.2f}s")


# 4 ---------------------------------------------------- send a file up / get it back
def demo_files(c):
    src = OUT / "file.wav"
    up = c.call("upload", {"name": "myfile.wav"}, data=src.read_bytes())
    print("upload ->", up.meta)
    down = c.call("download", {"name": "myfile.wav"})
    (OUT / "roundtrip.wav").write_bytes(down.data)
    print("download ->", len(down.data), "bytes, identical:",
          down.data == src.read_bytes())


# 5 --------------------------- stream IN (mic) while receiving results OUT
def demo_stream_in(c):
    call = c.start("transcribe", end=False)            # keep request side open

    def feed():                                        # pretend microphone
        for _ in range(5):
            call.send(data=b"\x00" * 3200)             # one "mic chunk"
            time.sleep(0.1)
        call.finish()                                  # tell server we're done

    threading.Thread(target=feed, daemon=True).start()
    for m in call:                                     # results arrive concurrently
        print("transcribe ->", m.meta)


# 6 ------------------------------------------------------------- cancel mid-job
def demo_cancel(c):
    call = c.start("count", {"n": 100})
    for m in call:
        print("count ->", m.meta["progress"])
        if m.meta["progress"] >= 3:
            call.cancel()                              # server stops, stream ends cleanly
    print("cancelled after 3 steps")


# 7 ------------------------------------- several calls at once on one connection
def demo_concurrent(c):
    results = {}

    def run(name, text):
        results[name] = sum(len(m.data) for m in c.start("tts_stream", {"text": text}))

    ts = [threading.Thread(target=run, args=(n, t)) for n, t in
          [("a", "first request"), ("b", "second request, longer"), ("c", "third")]]
    [t.start() for t in ts]; [t.join() for t in ts]
    print("concurrent ->", results)


# 8 ----------------------------------------------- change server-side settings
def demo_config(c):
    print("config ->", c.call("set_config", {"voice": "alice", "device": "cuda"}).meta)


# 9 ---------------------------------------------------------------------- errors
def demo_errors(c):
    try:
        c.call("boom")
    except RemoteError as e:
        print("RemoteError ->", e)
    try:
        c.call("does_not_exist")
    except RemoteError as e:
        print("RemoteError ->", e)
    try:
        c.call("count", {"n": 100}, timeout=0.05)      # reply slower than timeout
    except TimeoutError as e:
        print("TimeoutError ->", e)


if __name__ == "__main__":
    url, token = sys.argv[1], sys.argv[2]
    c = Client(url, token)
    for demo in (demo_ping, demo_tts_file, demo_tts_stream, demo_files,
                 demo_stream_in, demo_cancel, demo_concurrent, demo_config, demo_errors):
        print(f"\n--- {demo.__name__}")
        demo(c)
    c.close()
