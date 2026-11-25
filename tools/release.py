#!/usr/bin/env python3
"""Package the standalone ACP transport as an immutable local Go module proxy release."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MODULE = "agent-acp.local/transport"


def source_paths():
    # Only Go sources, documented fixtures and distributable metadata belong in the module.
    names = {"go.mod", "go.sum", "LICENSE", "NOTICE", "README.md", "UPSTREAM.md"}
    paths = [p for p in ROOT.iterdir() if p.name in names or p.suffix == ".go"]
    for folder in ("testdata", "example"):
        path = ROOT / folder
        if path.exists():
            paths.extend(p for p in path.rglob("*") if p.is_file() and not any(x.startswith(".") for x in p.relative_to(path).parts))
    for path in paths:
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(ROOT):
            raise ValueError(f"Expected a regular release input: {path.relative_to(ROOT)}")
    return sorted(paths)


def write_release(version, output, timestamp):
    direct = (ROOT / "go.mod").read_text(encoding="utf-8")
    if not re.search(rf"^module\s+{re.escape(MODULE)}\s*$", direct, re.MULTILINE):
        raise ValueError("Unexpected Go module identity")
    if re.search(r"^replace\s", direct, re.MULTILINE):
        raise ValueError("The release module cannot rely on checkout replacements")
    if not (ROOT / "LICENSE").is_file() or not (ROOT / "README.md").is_file():
        raise ValueError("The distribution requires its license and README")
    if output.exists():
        raise FileExistsError(f"Release destination already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    tag = "v" + version
    with tempfile.TemporaryDirectory(prefix=".transport-build-", dir=output.parent) as tmp:
        stage = Path(tmp) / "release"
        base = stage / "go" / MODULE / "@v"
        base.mkdir(parents=True)
        (base / "list").write_text(tag + "\n", encoding="utf-8")
        (base / (tag + ".info")).write_text(json.dumps({"Version": tag, "Time": timestamp}) + "\n", encoding="utf-8")
        shutil.copyfile(ROOT / "go.mod", base / (tag + ".mod"))
        snapshot = hashlib.sha256()
        with zipfile.ZipFile(base / (tag + ".zip"), "w") as archive:
            for path in source_paths():
                name = path.relative_to(ROOT).as_posix()
                data = path.read_bytes()
                snapshot.update(name.encode() + b"\x00" + hashlib.sha256(data).digest())
                member = zipfile.ZipInfo(f"{MODULE}@{tag}/{name}")
                member.create_system = 3
                member.external_attr = 0o100644 << 16
                archive.writestr(member, data, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
        digest = {p.relative_to(stage).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(base.iterdir())}
        manifest = {"name": "agent-acp-transport", "goModule": MODULE, "version": version, "releaseTime": timestamp,
                    "sourceSnapshotSHA256": snapshot.hexdigest(), "sha256": digest}
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        stage.rename(output)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="Semantic version without a v prefix")
    parser.add_argument("output", type=Path, help="New release directory")
    parser.add_argument("--epoch", type=int, default=os.environ.get("SOURCE_DATE_EPOCH"),
                        help="Release timestamp in Unix seconds; defaults to SOURCE_DATE_EPOCH or current UTC")
    args = parser.parse_args()
    if not re.fullmatch(r"(0|1)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", args.version):
        parser.error("Expected a major-0 or major-1 semantic version for this Go module path")
    when = datetime.fromtimestamp(args.epoch, timezone.utc) if args.epoch is not None else datetime.now(timezone.utc)
    print(write_release(args.version, args.output.resolve(), when.replace(microsecond=0).isoformat().replace("+00:00", "Z")))
