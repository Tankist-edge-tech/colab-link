"""example_gui.py - LOCAL PC. Sliders -> Colab -> streamed audio back.

    python example_gui.py wss://xxxx.trycloudflare.com <TOKEN>

The rule for any GUI: never call the client on the UI thread.
  UI thread  --starts-->  worker thread (iterates the call)  --queue-->  UI thread polls it
"""
import queue, sys, threading, tkinter as tk, wave
from dataclasses import dataclass, asdict
from pathlib import Path

from link import Client, RemoteError

try:                                   # optional live playback: pip install sounddevice
    import sounddevice as sd
except ImportError:
    sd = None


@dataclass
class TTSParams:
    text: str
    pitch: float = 1.0
    speed: float = 1.0


class App:
    def __init__(self, root, client):
        self.c, self.root = client, root
        self.events = queue.Queue()    # worker -> UI messages: ("audio", bytes) / ("status", str)
        self.call = None               # current call, so Stop can cancel it
        self.pcm = bytearray()

        self.text = tk.Entry(root, width=40); self.text.insert(0, "hello from the GUI")
        self.pitch = tk.Scale(root, from_=0.5, to=2.0, resolution=0.05,
                              orient="horizontal", label="pitch")
        self.speed = tk.Scale(root, from_=0.5, to=2.0, resolution=0.05,
                              orient="horizontal", label="speed")
        self.pitch.set(1.0); self.speed.set(1.0)
        self.status = tk.Label(root, text="ready")
        go = tk.Button(root, text="Generate", command=self.generate)
        stop = tk.Button(root, text="Stop", command=self.stop)
        for w in (self.text, self.pitch, self.speed, go, stop, self.status):
            w.pack(fill="x", padx=8, pady=3)
        self.poll()

    # --- UI thread: read widgets, start worker, return immediately
    def generate(self):
        params = TTSParams(self.text.get(), self.pitch.get(), self.speed.get())
        self.pcm.clear()
        threading.Thread(target=self.work, args=(asdict(params),), daemon=True).start()

    def stop(self):
        if self.call:
            self.call.cancel()

    # --- worker thread: blocking iteration is fine here
    def work(self, params):
        self.events.put(("status", "generating..."))
        try:
            self.call = self.c.start("tts_stream", params)
            for m in self.call:
                self.events.put(("audio", m.data))
            self.events.put(("status", "done"))
        except (RemoteError, ConnectionError) as e:
            self.events.put(("status", f"error: {e}"))
        finally:
            self.call = None
            self.events.put(("finished", b""))

    # --- UI thread: drain the queue every 30 ms (the only safe way to touch widgets)
    def poll(self):
        try:
            while True:
                kind, val = self.events.get_nowait()
                if kind == "audio":
                    self.pcm += val
                    if sd:                       # play each chunk as it arrives
                        sd.play(memoryview(val).cast("h"), 24000)   # simple; use OutputStream for gapless
                elif kind == "status":
                    self.status.config(text=val)
                elif kind == "finished":
                    with wave.open("out_gui.wav", "wb") as w:
                        w.setnchannels(1); w.setsampwidth(2); w.setframerate(24000)
                        w.writeframes(bytes(self.pcm))
        except queue.Empty:
            pass
        self.root.after(30, self.poll)


if __name__ == "__main__":
    client = Client(sys.argv[1], sys.argv[2])
    root = tk.Tk(); root.title("TTS client")
    App(root, client)
    root.mainloop()
