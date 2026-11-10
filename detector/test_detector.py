"""Tests that need no download and no GPU:  python -m unittest test_detector -v"""
import io
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

import numpy as np
from PIL import Image

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402
import server as S  # noqa: E402
import train_classifier as T  # noqa: E402


def tiny_backbone(path):
    """A stand-in for the base model: the mean colour of the image (3 numbers)."""
    import onnx
    from onnx import TensorProto, helper
    node = helper.make_node("ReduceMean", ["input"], ["out"], axes=[2, 3], keepdims=0)
    g = helper.make_graph([node], "g", [helper.make_tensor_value_info("input", TensorProto.FLOAT, ["b", 3, 224, 224])],
                          [helper.make_tensor_value_info("out", TensorProto.FLOAT, ["b", 3])])
    m = helper.make_model(g, opset_imports=[helper.make_opsetid("", 13)])
    m.ir_version = 8
    onnx.save(m, path)


def jpeg(color):
    b = io.BytesIO()
    Image.new("RGB", (64, 48), color).save(b, "JPEG")
    return b.getvalue()


class YoloxDecode(unittest.TestCase):
    def encode(self, size, boxes):
        """The inverse of decode_yolox: a raw output holding the given (cls, cx, cy, w, h) boxes."""
        grid, stride = C._grids(size)
        raw = np.zeros((grid.shape[0], 5 + 3), np.float32)
        raw[:, 4] = 0.001
        for cls, cx, cy, w, h in boxes:
            i = int(np.argmin(np.abs(grid[:, 0] * stride[:, 0] - cx) + np.abs(grid[:, 1] * stride[:, 0] - cy) + 1000 * (stride[:, 0] != 16)))
            raw[i, 0:2] = (np.array([cx, cy]) / stride[i]) - grid[i]
            raw[i, 2:4] = np.log(np.array([w, h]) / stride[i])
            raw[i, 4] = 0.9
            raw[i, 5 + cls] = 0.95
        return raw

    def test_boxes_come_back_in_image_fractions(self):
        raw = self.encode(416, [(1, 208, 208, 100, 200)])
        # an 832x832 picture shrunk by 0.5 to fit: in the picture the box is 200x400 px around (416, 416)
        out = C.yolox_boxes(raw, 416, 0.5, 832, 832)
        self.assertEqual(len(out), 1)
        cls, score, x1, y1, x2, y2 = out[0]
        self.assertEqual(cls, 1)
        self.assertAlmostEqual(score, 0.855, places=2)
        for got, want in zip((x1, y1, x2, y2), (0.38, 0.26, 0.62, 0.74)):
            self.assertAlmostEqual(got, want, delta=0.02)

    def test_nothing_below_the_threshold_and_duplicates_collapse(self):
        raw = self.encode(416, [(0, 100, 100, 60, 60)])
        self.assertEqual(C.yolox_boxes(raw, 416, 1.0, 416, 416, conf=0.95), [])
        raw2 = raw.copy()
        j = int(np.argmax(raw[:, 4]))
        k = (j + 1) % raw.shape[0] if (j + 1) % raw.shape[0] != j else 0
        raw2[k] = raw[j]  # the same box predicted by a neighbour cell
        self.assertEqual(len(C.yolox_boxes(raw2, 416, 1.0, 416, 416)), 1)

    def test_wrong_size_is_refused(self):
        with self.assertRaises(ValueError):
            C.decode_yolox(np.zeros((10, 8), np.float32), 416)


class Providers(unittest.TestCase):
    """The choice between CPU and GPU, with stand-ins for onnxruntime (there is no GPU here)."""

    def setUp(self):
        import onnxruntime as ort
        self.ort = ort
        self.real = (ort.InferenceSession, ort.get_available_providers)
        self.d = tempfile.mkdtemp()
        self.path = os.path.join(self.d, "b.onnx")
        tiny_backbone(self.path)

    def tearDown(self):
        self.ort.InferenceSession, self.ort.get_available_providers = self.real

    def fake(self, gpu_delay, gpu_scale=1.0, gpu_fails=False):
        real_cls = self.real[0]

        class Fake:
            def __init__(self, path, so=None, providers=None):
                self.inner = real_cls(path, providers=["CPUExecutionProvider"])
                self.p = providers[0]
                if self.p != "CPUExecutionProvider" and gpu_fails:
                    raise RuntimeError("no such GPU")

            def get_inputs(self): return self.inner.get_inputs()
            def get_providers(self): return [self.p]

            def run(self, a, b):
                out = self.inner.run(a, b)
                if self.p != "CPUExecutionProvider":
                    time.sleep(gpu_delay)
                    out = [o * gpu_scale for o in out]
                else:
                    time.sleep(0.01)
                return out
        self.ort.InferenceSession = Fake
        self.ort.get_available_providers = lambda: ["CUDAExecutionProvider", "CPUExecutionProvider"]

    def test_faster_gpu_with_same_answers_wins(self):
        self.fake(gpu_delay=0.001)
        _, info = C.make_session(self.path, "auto")
        self.assertEqual(info["provider"], "CUDAExecutionProvider")

    def test_gpu_that_gives_other_answers_is_not_used(self):
        self.fake(gpu_delay=0.001, gpu_scale=1.5)
        _, info = C.make_session(self.path, "auto")
        self.assertEqual(info["provider"], "CPUExecutionProvider")
        self.assertFalse(info["candidates"]["CUDAExecutionProvider"]["same_as_cpu"])

    def test_gpu_that_is_not_faster_is_not_used_in_auto_but_is_when_asked(self):
        self.fake(gpu_delay=0.02)
        self.assertEqual(C.make_session(self.path, "auto")[1]["provider"], "CPUExecutionProvider")
        self.assertEqual(C.make_session(self.path, "gpu")[1]["provider"], "CUDAExecutionProvider")

    def test_gpu_that_fails_falls_back_to_cpu(self):
        self.fake(gpu_delay=0.0, gpu_fails=True)
        _, info = C.make_session(self.path, "gpu")
        self.assertEqual(info["provider"], "CPUExecutionProvider")
        self.assertIn("error", info["candidates"]["CUDAExecutionProvider"])

    def test_cpu_only_never_looks_at_gpus(self):
        self.fake(gpu_delay=0.0)
        self.assertEqual(C.make_session(self.path, "cpu")[1]["candidates"], {})


class Trainer(unittest.TestCase):
    def test_fit_separates_two_clusters(self):
        rng = np.random.default_rng(1)
        X = np.vstack([rng.normal(0, 1, (30, 8)), rng.normal(3, 1, (30, 8))]).astype(np.float32)
        y = np.array([0] * 30 + [1] * 30)
        W, b, ep = T.fit(X, y, 2, np.ones(2, np.float32), 300, 1e-3, X, y)
        self.assertGreaterEqual((C.softmax(X @ W.T + b).argmax(1) == y).mean(), 0.95)


class Server(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp()
        os.makedirs(os.path.join(cls.root, C.BASE_DIR))
        tiny_backbone(os.path.join(cls.root, C.BASE_DIR, C.BACKBONE))
        # train a head on flat red / green / blue pictures, through the real code path
        data = os.path.join(cls.root, "data")
        for name, col in (("rojo", (230, 20, 20)), ("verde", (20, 230, 20)), ("azul", (20, 20, 230))):
            os.makedirs(os.path.join(data, name))
            for i in range(8):
                Image.new("RGB", (64, 48), tuple(min(255, max(0, c + i * 2)) for c in col)).save(os.path.join(data, name, f"{i}.jpg"))
        sys.argv = ["t", "--data", data, "--name", "colores", "--models", cls.root, "--device", "cpu"]
        T.main()
        cls.reg = S.Registry(cls.root, "cpu", 1)
        cls.httpd = S.ThreadingHTTPServer(("127.0.0.1", 0), S.make_handler(cls.reg, "secreto", 1 << 20, threading.BoundedSemaphore(2)))
        cls.url = f"http://127.0.0.1:{cls.httpd.server_address[1]}"
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def call(self, path, data=None, token="secreto"):
        req = urllib.request.Request(self.url + path, data=data, headers={"Authorization": f"Bearer {token}"} if token else {})
        try:
            with urllib.request.urlopen(req) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_classifies(self):
        code, res = self.call("/detect?model=colores", jpeg((10, 200, 30)))
        self.assertEqual(code, 200)
        self.assertEqual(max(res["scores"], key=res["scores"].get), "verde")
        self.assertEqual(res["kind"], "embed")

    def test_needs_the_token(self):
        self.assertEqual(self.call("/detect?model=colores", jpeg((1, 2, 3)), token="otro")[0], 401)
        self.assertEqual(self.call("/detect?model=colores", jpeg((1, 2, 3)), token=None)[0], 401)
        self.assertEqual(self.call("/models", token=None)[0], 401)
        code, res = self.call("/health", token=None)
        self.assertEqual((code, res), (200, {"ok": True}))  # up or not, nothing else
        self.assertIn("colores", self.call("/health")[1]["models"])

    def test_bad_requests(self):
        self.assertEqual(self.call("/detect?model=nada", jpeg((1, 2, 3)))[0], 404)
        self.assertEqual(self.call("/detect?model=..%2Fx", jpeg((1, 2, 3)))[0], 404)
        self.assertEqual(self.call("/detect?model=_base", jpeg((1, 2, 3)))[0], 404)
        self.assertEqual(self.call("/detect?model=colores", b"not an image")[0], 400)
        self.assertEqual(self.call("/detect?model=colores", b"x" * ((1 << 20) + 1))[0], 413)

    def test_a_retrained_model_is_picked_up(self):
        data = os.path.join(self.root, "data2")
        shutil.copytree(os.path.join(self.root, "data"), data)
        train = lambda: (setattr(sys, "argv", ["t", "--data", data, "--name", "rehecho", "--models", self.root, "--device", "cpu"]), T.main())  # noqa: E731
        train()
        before = self.call("/detect?model=rehecho", jpeg((10, 200, 30)))[1]["scores"]
        time.sleep(0.05)
        os.rename(os.path.join(data, "verde"), os.path.join(data, "amarillo"))
        train()
        after = self.call("/detect?model=rehecho", jpeg((10, 200, 30)))[1]["scores"]
        self.assertIn("verde", before)
        self.assertIn("amarillo", after)


if __name__ == "__main__":
    unittest.main()
