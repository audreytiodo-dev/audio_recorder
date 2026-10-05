#!/usr/bin/env python3
"""Deduplication des enregistrements telecharges.

Regroupe les fichiers .wav par CONTENU identique (MD5) ET MEME APPEL
(meme appelant + horodatage a quelques secondes pres), puis ne garde qu'un
exemplaire par appel — en privilegiant la version 'agent/extension' sur la
version 'Queue'.

Securites :
  - les fichiers vides (0 octet) sont ignores (appels distincts, pas des doublons) ;
  - deux fichiers identiques en octets mais issus d'appels differents ne sont PAS touches.

Mode par defaut : DRY-RUN (n'efface rien, ecrit juste dedupe_plan.txt).
Application      : python dedupe_recordings.py --apply
  -> deplace les doublons vers _doublons_quarantine/ (reversible), aucune suppression definitive.
"""
import hashlib
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

ROOT = "recordings/data"
QUARANTINE = "_doublons_quarantine"
APPLY = "--apply" in sys.argv
WINDOW = 5  # ecart max (secondes) entre deux legs d'un meme appel

NAME_RE = re.compile(r"^(\d{2}-\d{2}-\d{4})_(\d{2}-\d{2}-\d{2})_(.+?)_to_(.+)_id(\d+)\.wav$")


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def parse(name):
    m = NAME_RE.match(name)
    if not m:
        return None
    d, t, frm, dest, rid = m.groups()
    try:
        ts = int(datetime.strptime(f"{d}_{t}", "%m-%d-%Y_%H-%M-%S").timestamp())
    except Exception:
        ts = None
    return {"from": frm, "dest": dest, "id": int(rid), "ts": ts, "is_queue": "Queue" in dest}


# 1) Collecte (exclut les fichiers vides)
files, empty = [], 0
for dp, _, fs in os.walk(ROOT):
    for f in fs:
        if not f.endswith(".wav"):
            continue
        p = os.path.join(dp, f)
        if os.path.getsize(p) == 0:
            empty += 1
            continue
        files.append((p, os.path.getsize(p), f))

# 2) Groupes de contenu identique (taille puis MD5)
by_size = defaultdict(list)
for p, sz, f in files:
    by_size[sz].append((p, f))
content = defaultdict(list)
for sz, items in by_size.items():
    if len(items) < 2:
        continue
    for p, f in items:
        content[(sz, md5(p))].append((p, f, parse(f)))

# 3) Dans chaque groupe : clusters 'meme appel', puis choix du gardien
to_remove = []
clusters_total = clusters_multi = clusters_no_agent = 0
cross_call_files = 0  # contenu identique mais appels differents -> NON touches

for group in content.values():
    if len(group) < 2:
        continue
    used = [False] * len(group)
    for i in range(len(group)):
        if used[i]:
            continue
        cluster = [group[i]]
        used[i] = True
        pi = group[i][2]
        for j in range(i + 1, len(group)):
            if used[j]:
                continue
            pj = group[j][2]
            if (pi and pj and pi["from"] == pj["from"]
                    and pi["ts"] and pj["ts"] and abs(pi["ts"] - pj["ts"]) <= WINDOW):
                cluster.append(group[j])
                used[j] = True
        if len(cluster) < 2:
            cross_call_files += 1
            continue
        clusters_total += 1
        if len(cluster) > 2:
            clusters_multi += 1
        agents = [c for c in cluster if c[2] and not c[2]["is_queue"]]
        if not agents:
            clusters_no_agent += 1
        pool = agents if agents else cluster
        keeper = sorted(pool, key=lambda c: c[2]["id"] if c[2] else 0)[0]
        for c in cluster:
            if c[0] != keeper[0]:
                to_remove.append((c[0], keeper[0]))

reclaim = sum(os.path.getsize(p) for p, _ in to_remove)
print(f"Fichiers .wav non vides        : {len(files)}")
print(f"Fichiers vides (0 octet) ignores : {empty}")
print(f"Clusters 'meme appel' avec doublons : {clusters_total}")
print(f"  - dont >2 copies            : {clusters_multi}")
print(f"  - sans version agent (Queue seule) : {clusters_no_agent}")
print(f"Contenu identique mais appels DIFFERENTS (non touches) : {cross_call_files}")
print(f"==> Fichiers a retirer : {len(to_remove)}  |  espace recupere : {reclaim / 1e9:.2f} Go")

with open("dedupe_plan.txt", "w", encoding="utf-8") as out:
    for rem, keep in to_remove:
        out.write(f"REMOVE {rem}\n  KEEP {keep}\n")
print("Plan complet ecrit dans dedupe_plan.txt")

if APPLY:
    moved = 0
    for rem, _ in to_remove:
        dest = os.path.join(QUARANTINE, os.path.relpath(rem, ROOT))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        os.replace(rem, dest)
        moved += 1
    print(f"[APPLIQUE] {moved} doublons deplaces vers {QUARANTINE}/ (reversible).")
else:
    print("\n[DRY-RUN] Aucun fichier modifie.")
    print("Exemples de decisions (5 premiers) :")
    for rem, keep in to_remove[:5]:
        print(f"  RETIRER {os.path.basename(rem)}")
        print(f"   GARDER {os.path.basename(keep)}")
    print("\nPour appliquer : python dedupe_recordings.py --apply")
