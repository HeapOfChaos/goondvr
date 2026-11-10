"""Shared pieces of the detector: choosing CPU or GPU, preparing images, reading models.

Only numpy, Pillow and onnxruntime are needed (onnx only to cut the base model once).
"""
import io
import json
import os
import re
import statistics
import time

import numpy as np
from PIL import Image

BASE_DIR = "_base"  # models/_base holds the shared backbone; names that start with "_" are never served
BACKBONE = "mobilenetv2_features.onnx"
EMBED_SIZE = 224
MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)

COCO_LABELS = [
    "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train", "truck", "boat", "traffic light",
    "fire hydrant", "stop sign", "parking meter", "bench", "bird", "cat", "dog", "horse", "sheep", "cow",
    "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella", "handbag", "tie", "suitcase", "frisbee",
    "skis", "snowboard", "sports ball", "kite", "baseball bat", "baseball glove", "skateboard", "surfboard",
    "tennis racket", "bottle", "wine glass", "cup", "fork", "knife", "spoon", "bowl", "banana", "apple",
    "sandwich", "orange", "broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair", "couch",
    "potted plant", "bed", "dining table", "toilet", "tv", "laptop", "mouse", "remote", "keyboard", "cell phone",
    "microwave", "oven", "toaster", "sink", "refrigerator", "book", "clock", "vase", "scissors", "teddy bear",
    "hair drier", "toothbrush",
]

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


# ---------------------------------------------------------------- CPU or GPU

GPU_PROVIDERS = ("CUDAExecutionProvider", "DmlExecutionProvider", "CoreMLExecutionProvider")


def _probe_input(sess):
    """A fixed input for the model (dynamic sizes become 1), the same for every provider."""
    rng = np.random.default_rng(0)
    feed = {}
    for i in sess.get_inputs():
        shape = [d if isinstance(d, int) and d > 0 else 1 for d in i.shape]
        feed[i.name] = (rng.random(shape) * 255 if len(shape) == 4 and shape[1] == 3 and shape[2] > 100
                        else rng.random(shape)).astype(np.float32)
    return feed


def _median_ms(sess, feed, runs=15, warm=3):
    for _ in range(warm):
        sess.run(None, feed)
    t = []
    for _ in range(runs):
        t0 = time.perf_counter()
        sess.run(None, feed)
        t.append((time.perf_counter() - t0) * 1000)
    return statistics.median(t)


def make_session(path, device="auto", threads=2):
    """Open an ONNX model on the best provider. Returns (session, info).

    device: "auto" = the fastest provider whose answers match the CPU's (a GPU is used only if it is
    at least 20% faster: a small model is often quicker on the CPU), "cpu", or "gpu" (GPU if it works).
    The CPU uses `threads` threads, so that the detector stays light.
    """
    import onnxruntime as ort

    so = ort.SessionOptions()
    so.intra_op_num_threads = max(1, threads)
    so.inter_op_num_threads = 1
    so.log_severity_level = 3
    cpu = ort.InferenceSession(path, so, providers=["CPUExecutionProvider"])
    info = {"provider": "CPUExecutionProvider", "candidates": {}}
    if device == "cpu":
        return cpu, info
    gpus = [p for p in GPU_PROVIDERS if p in ort.get_available_providers()]
    if not gpus:
        info["note"] = "no GPU provider is installed: using the CPU"
        return cpu, info

    feed = _probe_input(cpu)
    ref = cpu.run(None, feed)
    best, best_ms = cpu, _median_ms(cpu, feed)
    info["candidates"]["CPUExecutionProvider"] = {"ms": round(best_ms, 2)}
    cpu_ms = best_ms
    for name in gpus:
        try:
            so2 = ort.SessionOptions()
            so2.log_severity_level = 3
            if name == "DmlExecutionProvider":  # DirectML needs these two
                so2.enable_mem_pattern = False
                so2.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
            s = ort.InferenceSession(path, so2, providers=[name, "CPUExecutionProvider"])
            if s.get_providers()[0] != name:
                raise RuntimeError("the provider was not used")
            out = s.run(None, feed)
            same = all(np.allclose(a, b, rtol=1e-2, atol=1e-2) for a, b in zip(ref, out))
            ms = _median_ms(s, feed)
            info["candidates"][name] = {"ms": round(ms, 2), "same_as_cpu": bool(same)}
            wins = ms < cpu_ms * 0.8 if device == "auto" else ms < best_ms or best is cpu
            if same and wins and (best is cpu or ms < best_ms):
                best, best_ms, info["provider"] = s, ms, name
        except Exception as e:  # a GPU that does not work is not a reason to stop
            info["candidates"][name] = {"error": str(e)[:200]}
    return best, info


# ---------------------------------------------------------------- images

def open_image(data):
    img = Image.open(io.BytesIO(data))
    img.draft("RGB", (1280, 1280))  # JPEG: decode at a reduced size, much cheaper for big frames
    return img.convert("RGB")


def prep_embed(img, size=EMBED_SIZE):
    """RGB image -> (3,size,size) float32 for the base model (the whole frame, squeezed to a square)."""
    a = np.asarray(img.resize((size, size), Image.BILINEAR), dtype=np.float32) / 255.0
    return ((a - MEAN) / STD).transpose(2, 0, 1)


def prep_yolox(img, size):
    """RGB image -> ((1,3,size,size) float32 BGR 0..255, ratio): the way YOLOX expects it (padded with 114)."""
    w, h = img.size
    r = min(size / h, size / w)
    nw, nh = max(1, int(w * r)), max(1, int(h * r))
    canvas = np.full((size, size, 3), 114, dtype=np.float32)
    canvas[:nh, :nw] = np.asarray(img.resize((nw, nh), Image.BILINEAR), dtype=np.float32)[:, :, ::-1]
    return canvas.transpose(2, 0, 1)[None], r


# ---------------------------------------------------------------- YOLOX output

def _grids(size, strides=(8, 16, 32)):
    g, s = [], []
    for st in strides:
        n = size // st
        xv, yv = np.meshgrid(np.arange(n), np.arange(n))
        grid = np.stack((xv, yv), 2).reshape(-1, 2)
        g.append(grid)
        s.append(np.full((grid.shape[0], 1), st))
    return np.concatenate(g, 0).astype(np.float32), np.concatenate(s, 0).astype(np.float32)


def decode_yolox(raw, size):
    """(N,5+C) raw head output -> boxes as centre x,y,width,height in input pixels (same layout)."""
    grid, stride = _grids(size)
    out = np.array(raw, dtype=np.float32, copy=True)
    if out.shape[0] != grid.shape[0]:
        raise ValueError(f"the model gave {out.shape[0]} boxes, {grid.shape[0]} expected for a {size}px input")
    out[:, :2] = (out[:, :2] + grid) * stride
    out[:, 2:4] = np.exp(out[:, 2:4]) * stride
    return out


def nms(boxes, scores, thr):
    """Greedy non-maximum suppression. boxes (N,4) x1,y1,x2,y2. Returns the indices kept."""
    order = scores.argsort()[::-1]
    keep = []
    area = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
    while order.size:
        i = order[0]
        keep.append(int(i))
        if order.size == 1:
            break
        rest = order[1:]
        xx1 = np.maximum(boxes[i, 0], boxes[rest, 0])
        yy1 = np.maximum(boxes[i, 1], boxes[rest, 1])
        xx2 = np.minimum(boxes[i, 2], boxes[rest, 2])
        yy2 = np.minimum(boxes[i, 3], boxes[rest, 3])
        inter = np.maximum(0, xx2 - xx1) * np.maximum(0, yy2 - yy1)
        iou = inter / (area[i] + area[rest] - inter + 1e-9)
        order = rest[iou <= thr]
    return keep


def yolox_boxes(raw, size, ratio, img_w, img_h, conf=0.25, nms_thr=0.45, max_boxes=100):
    """Boxes found in a YOLOX output: [(class_id, score, x1, y1, x2, y2)] with the box as 0..1 of the image."""
    d = decode_yolox(raw, size)
    scores = d[:, 4:5] * d[:, 5:]
    cls = scores.argmax(1)
    sc = scores[np.arange(len(cls)), cls]
    m = sc > conf
    if not m.any():
        return []
    d, cls, sc = d[m], cls[m], sc[m]
    xyxy = np.stack([d[:, 0] - d[:, 2] / 2, d[:, 1] - d[:, 3] / 2, d[:, 0] + d[:, 2] / 2, d[:, 1] + d[:, 3] / 2], 1) / ratio
    xyxy[:, [0, 2]] = np.clip(xyxy[:, [0, 2]], 0, img_w) / img_w
    xyxy[:, [1, 3]] = np.clip(xyxy[:, [1, 3]], 0, img_h) / img_h
    keep = nms(xyxy, sc, nms_thr)[:max_boxes]  # across classes, like the YOLOX demo: one object, one label
    return [(int(cls[i]), float(sc[i]), *[float(v) for v in xyxy[i]]) for i in keep]


# ---------------------------------------------------------------- models on disk

def read_labels(folder, meta):
    labels = meta.get("labels")
    if labels:
        return list(labels)
    p = os.path.join(folder, "labels.txt")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip()]
    return []


def write_model_folder(folder, meta, labels):
    os.makedirs(folder, exist_ok=True)
    meta = dict(meta, labels=list(labels))
    tmp = os.path.join(folder, "meta.json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=1, ensure_ascii=False)
    with open(os.path.join(folder, "labels.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(labels) + "\n")
    os.replace(tmp, os.path.join(folder, "meta.json"))  # meta.json last: the server reloads when it changes


def softmax(z):
    z = z - z.max(axis=-1, keepdims=True)
    e = np.exp(z)
    return e / e.sum(axis=-1, keepdims=True)
