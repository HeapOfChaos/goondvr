"""Downloads the two base models once (both Apache-2.0) into models/.

  models/_base/mobilenetv2_features.onnx   base of the classifiers you train (MobileNetV2, cut before its 1000-class layer)
  models/yolox_nano_coco/                  ready-to-use detector of 80 everyday things (person, cup, laptop...)

    python fetch_models.py [--models DIR]
"""
import argparse
import hashlib
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

MNV2_URL = "https://media.githubusercontent.com/media/onnx/models/main/validated/vision/classification/mobilenet/model/mobilenetv2-12.onnx"
MNV2_SHA = "c0c3f76d93fa3fd6580652a45618618a220fced18babf65774ed169de0432ad5"
YOLOX_URL = "https://github.com/Megvii-BaseDetection/YOLOX/releases/download/0.1.1rc0/yolox_nano.onnx"
YOLOX_SHA = "c789161ed43c8269fcd4e67c67eeeb4e80c622da2eb296a20bc6007bd18a0b7d"


def download(url, sha, dest):
    if os.path.exists(dest) and hashlib.sha256(open(dest, "rb").read()).hexdigest() == sha:
        return
    print("downloading", url, flush=True)
    tmp = dest + ".part"
    with urllib.request.urlopen(url, timeout=120) as r, open(tmp, "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
    got = hashlib.sha256(open(tmp, "rb").read()).hexdigest()
    if got != sha:
        os.remove(tmp)
        sys.exit(f"the file from {url} is not the expected one (sha256 {got})")
    os.replace(tmp, dest)


def cut_features(src, dest):
    """MobileNetV2 without its last layer: the output is the 1280 numbers that describe the image."""
    import onnx
    from onnx import TensorProto, helper
    m = onnx.load(src)
    gemm = [n for n in m.graph.node if n.op_type == "Gemm"][-1]
    feat, drop = gemm.input[0], {gemm.input[1], gemm.input[2]}
    m.graph.node.remove(gemm)
    for i in [i for i in m.graph.initializer if i.name in drop]:
        m.graph.initializer.remove(i)
    del m.graph.output[:]
    m.graph.output.append(helper.make_tensor_value_info(feat, TensorProto.FLOAT, ["batch_size", 1280]))
    onnx.checker.check_model(m)
    onnx.save(m, dest)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "models"))
    a = ap.parse_args()
    base = os.path.join(a.models, C.BASE_DIR)
    os.makedirs(base, exist_ok=True)

    feats = os.path.join(base, C.BACKBONE)
    if not os.path.exists(feats):
        raw = os.path.join(base, "mobilenetv2-12.onnx")
        download(MNV2_URL, MNV2_SHA, raw)
        cut_features(raw, feats)
        os.remove(raw)
    folder = os.path.join(a.models, "yolox_nano_coco")
    os.makedirs(folder, exist_ok=True)
    download(YOLOX_URL, YOLOX_SHA, os.path.join(folder, "model.onnx"))
    C.write_model_folder(folder, {"kind": "yolox", "size": 416, "conf": 0.3, "nms": 0.45}, C.COCO_LABELS)
    print("ready:", os.listdir(a.models))


if __name__ == "__main__":
    main()
