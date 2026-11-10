"""Detection server: receives an image, answers with what is in it.

    python server.py                       local, on this computer (127.0.0.1:8099)
    python server.py --host 0.0.0.0 --token SECRET     reachable from other machines / a tunnel (Colab)

Models are folders under --models: models/<name>/meta.json plus the files of the model.
  kind "embed": whole-image classifier = shared base model + a small trained head (train_classifier.py)
  kind "yolox": boxes (YOLOX ONNX, model.onnx)
A model folder that changes is reloaded at the next request: no restart after training.
"""
import argparse
import hmac
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402


class Model:
    def __init__(self, folder, device, threads):
        self.folder = folder
        with open(os.path.join(folder, "meta.json"), encoding="utf-8") as f:
            meta = json.load(f)
        self.meta = meta
        self.kind = meta.get("kind")
        self.labels = C.read_labels(folder, meta)
        if not self.labels:
            raise ValueError("the model has no labels")
        if self.kind == "embed":
            base = os.path.join(os.path.dirname(folder), C.BASE_DIR, meta.get("backbone", C.BACKBONE))
            if not os.path.exists(base):
                raise ValueError(f"the base model {base} is missing: run fetch_models.py")
            self.sess, self.info = C.make_session(base, device, threads)
            with np.load(os.path.join(folder, "head.npz")) as head:
                self.W, self.b = head["W"].astype(np.float32), head["b"].astype(np.float32)
                self.mu, self.sd = head["mu"].astype(np.float32), head["sd"].astype(np.float32)
            if self.W.shape[0] != len(self.labels):
                raise ValueError("head.npz and labels do not match")
            self.size = int(meta.get("size", C.EMBED_SIZE))
        elif self.kind == "yolox":
            self.sess, self.info = C.make_session(os.path.join(folder, "model.onnx"), device, threads)
            self.size = int(meta.get("size", self.sess.get_inputs()[0].shape[-1]))
            self.conf = float(meta.get("conf", 0.25))
            self.nms = float(meta.get("nms", 0.45))
        else:
            raise ValueError(f"unknown kind {self.kind!r}")
        self.input = self.sess.get_inputs()[0].name

    def infer(self, img):
        if self.kind == "embed":
            x = C.prep_embed(img, self.size)[None]
            f = self.sess.run(None, {self.input: x})[0]
            p = C.softmax(((f - self.mu) / self.sd) @ self.W.T + self.b)[0]
            return {"scores": {l: round(float(s), 4) for l, s in zip(self.labels, p)}}
        x, r = C.prep_yolox(img, self.size)
        raw = self.sess.run(None, {self.input: x})[0][0]
        boxes, scores = [], {}
        for cid, sc, x1, y1, x2, y2 in C.yolox_boxes(raw, self.size, r, img.width, img.height, self.conf, self.nms):
            name = self.labels[cid] if cid < len(self.labels) else str(cid)
            scores[name] = max(scores.get(name, 0.0), round(sc, 4))
            boxes.append({"label": name, "score": round(sc, 4), "box": [round(v, 4) for v in (x1, y1, x2, y2)]})
        return {"scores": scores, "boxes": boxes}


class Registry:
    """Models loaded on demand, reloaded when their folder changes."""

    def __init__(self, root, device, threads):
        self.root, self.device, self.threads = root, device, threads
        self.lock = threading.Lock()
        self.loaded = {}  # name -> (stamp, Model)

    def _stamp(self, folder):
        return tuple(os.stat(os.path.join(folder, f)).st_mtime_ns
                     for f in sorted(os.listdir(folder)) if f.endswith((".json", ".npz", ".onnx")))

    def names(self):
        out = []
        for n in sorted(os.listdir(self.root)) if os.path.isdir(self.root) else []:
            if C.NAME_RE.match(n) and os.path.exists(os.path.join(self.root, n, "meta.json")):
                out.append(n)
        return out

    def get(self, name):
        if not C.NAME_RE.match(name or ""):
            raise KeyError(name)
        folder = os.path.join(self.root, name)
        if not os.path.exists(os.path.join(folder, "meta.json")):
            raise KeyError(name)
        stamp = self._stamp(folder)
        with self.lock:  # one at a time: loading a model benchmarks it
            hit = self.loaded.get(name)
            if hit and hit[0] == stamp:
                return hit[1]
            t0 = time.time()
            m = Model(folder, self.device, self.threads)
            self.loaded[name] = (stamp, m)
            print(f"model {name!r} ({m.kind}) loaded in {time.time() - t0:.1f}s on {m.info['provider']} {json.dumps(m.info['candidates'])}", flush=True)
            return m


def make_handler(reg, token, max_bytes, slots):
    class H(BaseHTTPRequestHandler):
        server_version = "goondvr-detector"

        def log_message(self, *a):  # quiet: one line per error is printed below
            pass

        def _send(self, code, obj):
            b = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def _authorized(self):
            if not token:
                return True
            got = self.headers.get("Authorization", "")
            return hmac.compare_digest(got.encode(), ("Bearer " + token).encode())

        def do_GET(self):
            u = urlparse(self.path)
            if u.path == "/health":
                if not self._authorized():
                    return self._send(200, {"ok": True})  # anyone may ask if it is up, nothing more
                return self._send(200, {"ok": True, "models": reg.names(), "device": reg.device})
            if not self._authorized():
                return self._send(401, {"error": "bad token"})
            if u.path == "/models":
                out = []
                for n in reg.names():
                    try:
                        m = reg.get(n)
                        out.append({"name": n, "kind": m.kind, "labels": m.labels, "provider": m.info["provider"],
                                    "candidates": m.info["candidates"]})
                    except Exception as e:
                        out.append({"name": n, "error": str(e)})
                return self._send(200, {"models": out})
            self._send(404, {"error": "not found"})

        def do_POST(self):
            u = urlparse(self.path)
            if u.path != "/detect":
                return self._send(404, {"error": "not found"})
            if not self._authorized():
                return self._send(401, {"error": "bad token"})
            try:
                n = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                n = 0
            if n <= 0 or n > max_bytes:
                return self._send(413, {"error": f"the image must be 1..{max_bytes} bytes"})
            body = self.rfile.read(n)
            name = (parse_qs(u.query).get("model") or [""])[0]
            try:
                model = reg.get(name)
            except KeyError:
                return self._send(404, {"error": f"model {name!r} not found"})
            except Exception as e:
                return self._send(500, {"error": f"model {name!r} cannot be loaded: {e}"})
            try:
                img = C.open_image(body)
            except Exception:
                return self._send(400, {"error": "not an image"})
            with slots:  # at most a few inferences at once: the machine stays light
                t0 = time.perf_counter()
                try:
                    res = model.infer(img)
                except Exception as e:
                    print("inference error:", e, flush=True)
                    return self._send(500, {"error": "inference failed"})
            res.update(model=name, kind=model.kind, provider=model.info["provider"],
                       ms=round((time.perf_counter() - t0) * 1000, 1))
            self._send(200, res)

    return H


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--models", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "models"))
    ap.add_argument("--token", default=os.environ.get("DETECTOR_TOKEN", ""))
    ap.add_argument("--device", choices=["auto", "cpu", "gpu"], default="auto",
                    help="auto: the fastest of CPU/GPU whose results match the CPU's (default)")
    ap.add_argument("--threads", type=int, default=2, help="CPU threads per model (default 2: light)")
    ap.add_argument("--parallel", type=int, default=2, help="inferences at the same time")
    ap.add_argument("--max-image-mb", type=int, default=8)
    a = ap.parse_args()

    if a.host not in ("127.0.0.1", "localhost", "::1") and not a.token:
        sys.exit("refusing to listen on the network without --token")
    reg = Registry(a.models, a.device, a.threads)
    srv = ThreadingHTTPServer((a.host, a.port), make_handler(reg, a.token, a.max_image_mb << 20, threading.BoundedSemaphore(a.parallel)))
    srv.daemon_threads = True
    print(f"detector on http://{a.host}:{a.port}  models: {reg.names() or 'none yet (run fetch_models.py)'}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
