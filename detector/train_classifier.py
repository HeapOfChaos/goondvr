"""Trains a classifier of whole images from your own examples, on this computer, in a few minutes.

    python train_classifier.py --data mis_imagenes --name mi_modelo

mis_imagenes/ has one folder per class, and the folder name is the class:
    mis_imagenes/en_vivo/*.jpg      mis_imagenes/pantalla_negra/*.jpg      mis_imagenes/otra_escena/*.jpg

How it works: a ready-made network (MobileNetV2) turns each image into 1280 numbers; this script trains the
small last layer that tells your classes apart. That needs no GPU and few images (20-30 per class already
works; more and more varied is better). The result goes to models/<name>/ and the server picks it up by itself.
"""
import argparse
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

EXT = (".jpg", ".jpeg", ".png", ".webp", ".bmp")


def list_dataset(root):
    classes = sorted(d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d)) and not d.startswith("."))
    files = {c: sorted(os.path.join(root, c, f) for f in os.listdir(os.path.join(root, c)) if f.lower().endswith(EXT)) for c in classes}
    return {c: f for c, f in files.items() if f}


def embed(sess, name, paths, flip=False, batch=32):
    out = []
    for i in range(0, len(paths), batch):
        xs = []
        for p in paths[i:i + batch]:
            try:
                img = C.open_image(open(p, "rb").read())
            except Exception as e:
                sys.exit(f"no se pudo leer la imagen {p}: {e}")
            if flip:
                img = img.transpose(0)  # PIL FLIP_LEFT_RIGHT
            xs.append(C.prep_embed(img))
        out.append(sess.run(None, {name: np.stack(xs)})[0])
    return np.concatenate(out) if out else np.zeros((0, 1280), np.float32)


def fit(X, y, K, weights, epochs, lam, Xv=None, yv=None, patience=30, lr=0.05):
    """Softmax regression with L2 and Adam. With a validation set it keeps the best epoch."""
    n, d = X.shape
    W, b = np.zeros((K, d), np.float32), np.zeros(K, np.float32)
    mW, vW, mb, vb = np.zeros_like(W), np.zeros_like(W), np.zeros_like(b), np.zeros_like(b)
    onehot = np.eye(K, dtype=np.float32)[y]
    sw = weights[y]
    sw = sw / sw.sum()
    best, best_ep, bad = (None, None, 1e9), 0, 0
    for ep in range(1, epochs + 1):
        P = C.softmax(X @ W.T + b)
        G = (P - onehot) * sw[:, None]
        gW, gb = G.T @ X + lam * W, G.sum(0)
        for p, g, m, v in ((W, gW, mW, vW), (b, gb, mb, vb)):
            m[:] = 0.9 * m + 0.1 * g
            v[:] = 0.999 * v + 0.001 * g * g
            p -= lr * (m / (1 - 0.9 ** ep)) / (np.sqrt(v / (1 - 0.999 ** ep)) + 1e-8)
        if Xv is not None:
            Pv = C.softmax(Xv @ W.T + b)
            loss = float(-np.log(Pv[np.arange(len(yv)), yv] + 1e-9).mean())
            if loss < best[2] - 1e-4:
                best, best_ep, bad = (W.copy(), b.copy(), loss), ep, 0
            else:
                bad += 1
                if bad >= patience:
                    break
    if Xv is None:
        return W, b, epochs
    return best[0], best[1], best_ep


def report(P, y, classes):
    K = len(classes)
    pred = P.argmax(1)
    cm = np.zeros((K, K), int)
    for t, p in zip(y, pred):
        cm[t, p] += 1
    print(f"\nAciertos en las imágenes de comprobación: {(pred == y).mean() * 100:.1f}%  ({len(y)} imágenes)")
    print("\nMatriz (filas = lo que era, columnas = lo que dijo el modelo):")
    w = max(len(c) for c in classes)
    print(" " * (w + 2) + "  ".join(f"{c[:8]:>8}" for c in classes))
    for i, c in enumerate(classes):
        print(f"{c:>{w}}  " + "  ".join(f"{v:>8}" for v in cm[i]))
    print("\nPor clase:")
    for i, c in enumerate(classes):
        tp, fp, fn = cm[i, i], cm[:, i].sum() - cm[i, i], cm[i].sum() - cm[i, i]
        print(f"  {c}: precisión {tp / max(1, tp + fp) * 100:.0f}%  recuperación {tp / max(1, tp + fn) * 100:.0f}%")
    print("\nSi pides una puntuación mínima, así queda (aciertos entre lo que el modelo se atreve a decir / imágenes que dice):")
    for thr in (0.5, 0.7, 0.9):
        sure = P.max(1) >= thr
        if sure.any():
            print(f"  mínimo {thr:.1f}: {(pred[sure] == y[sure]).mean() * 100:.1f}% de aciertos, responde en el {sure.mean() * 100:.0f}% de las imágenes")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, help="carpeta con una subcarpeta por clase")
    ap.add_argument("--name", required=True, help="nombre del modelo (se guarda en models/<nombre>)")
    ap.add_argument("--val-data", help="carpeta de comprobación con las mismas clases (mejor si son de otras transmisiones)")
    ap.add_argument("--val", type=float, default=0.2, help="parte de las imágenes que se aparta para comprobar si no hay --val-data")
    ap.add_argument("--models", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "models"))
    ap.add_argument("--device", choices=["auto", "cpu", "gpu"], default="auto")
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--epochs", type=int, default=600)
    ap.add_argument("--l2", type=float, default=1e-3)
    ap.add_argument("--seed", type=int, default=0)
    a = ap.parse_args()

    if not C.NAME_RE.match(a.name) or a.name.startswith("_"):
        sys.exit("el nombre solo puede llevar letras, números, _ . - y no empezar por _")
    base = os.path.join(a.models, C.BASE_DIR, C.BACKBONE)
    if not os.path.exists(base):
        sys.exit("falta el modelo base: ejecuta primero  python fetch_models.py")
    data = list_dataset(a.data)
    classes = list(data)
    if len(classes) < 2:
        sys.exit("hacen falta al menos 2 carpetas (clases) con imágenes")
    for c in classes:
        if len(data[c]) < 5:
            sys.exit(f"la clase {c!r} tiene {len(data[c])} imágenes: pon al menos 5 (mejor 30 o más)")
        if len(data[c]) < 20:
            print(f"aviso: la clase {c!r} tiene pocas imágenes ({len(data[c])}); funcionará mejor con 30 o más")
    print("clases:", ", ".join(f"{c} ({len(data[c])})" for c in classes))

    sess, info = C.make_session(base, a.device, a.threads)
    print("usando:", info["provider"], info["candidates"])
    name = sess.get_inputs()[0].name
    rng = np.random.default_rng(a.seed)

    tr_p, tr_y, va_p, va_y = [], [], [], []
    ext_val = list_dataset(a.val_data) if a.val_data else None
    for k, c in enumerate(classes):
        paths = list(data[c])
        if ext_val is not None:
            tr_p += paths
            tr_y += [k] * len(paths)
            va_p += ext_val.get(c, [])
            va_y += [k] * len(ext_val.get(c, []))
        else:
            rng.shuffle(paths)
            nv = max(1, int(round(len(paths) * a.val)))
            va_p += paths[:nv]
            va_y += [k] * nv
            tr_p += paths[nv:]
            tr_y += [k] * (len(paths) - nv)
    if ext_val is None:
        print("aviso: las imágenes de comprobación salen de las mismas carpetas; si son fotogramas seguidos de la misma\n"
              "       transmisión se parecen mucho a las de entrenamiento y el acierto sale más alto que en la realidad.\n"
              "       Para una cifra honesta usa --val-data con imágenes de otras transmisiones.")
    tr_y, va_y = np.array(tr_y), np.array(va_y)
    if len(va_y) == 0:
        sys.exit("la carpeta de comprobación no tiene imágenes de ninguna de las clases")

    print("calculando descriptores…", flush=True)
    Xtr = np.concatenate([embed(sess, name, tr_p), embed(sess, name, tr_p, flip=True)])
    ytr = np.concatenate([tr_y, tr_y])
    Xva = embed(sess, name, va_p)
    mu = Xtr.mean(0)
    sd = Xtr.std(0) + 1e-3
    Z = lambda X: (X - mu) / sd  # noqa: E731
    K = len(classes)
    counts = np.bincount(ytr, minlength=K).astype(np.float32)
    weights = counts.sum() / (K * counts)  # rare classes weigh more

    W, b, ep = fit(Z(Xtr), ytr, K, weights, a.epochs, a.l2, Z(Xva), va_y)
    report(C.softmax(Z(Xva) @ W.T + b), va_y, classes)

    # With the number of rounds found, learn again from ALL the images (the ones set aside too).
    Xall = np.concatenate([Xtr, Xva, embed(sess, name, va_p, flip=True)])
    yall = np.concatenate([ytr, va_y, va_y])
    counts = np.bincount(yall, minlength=K).astype(np.float32)
    W, b, _ = fit(Z(Xall), yall, K, counts.sum() / (K * counts), ep, a.l2)

    folder = os.path.join(a.models, a.name)
    C.write_model_folder(folder, {"kind": "embed", "size": C.EMBED_SIZE, "backbone": C.BACKBONE}, classes)
    np.savez(os.path.join(folder, "head.npz"), W=W, b=b, mu=mu, sd=sd)
    print(f"\nModelo guardado en {folder}\nSi el servidor está en marcha, ya lo usa (se recarga solo).")


if __name__ == "__main__":
    main()
