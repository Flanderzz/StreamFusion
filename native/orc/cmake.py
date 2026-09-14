#!/usr/bin/env python3
"""Use CMake >= 3.25, or cache the canonical binary wheel without a system installation."""
import fcntl
from pathlib import Path
import shutil
import subprocess
import sys
import venv

installed = shutil.which("cmake")
if installed:
    version = subprocess.check_output([installed, "--version"], text=True).split()[2]
    if tuple(int(part) for part in version.split(".")[:2]) >= (3, 25):
        print(installed)
        sys.exit(0)

cache = Path(sys.argv[1]) / "cmake-3.31.10"
cache.parent.mkdir(parents=True, exist_ok=True)
with (cache.parent / "cmake.lock").open("w") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    binary = cache / "bin" / "cmake"
    if not binary.exists():
        venv.create(cache, with_pip=True)
        subprocess.run([
            str(cache / "bin" / "python"), "-m", "pip", "--isolated", "install",
            "--index-url", "https://pypi.org/simple", "--only-binary=:all:", "--no-deps",
            "--disable-pip-version-check", "cmake==3.31.10",
        ], check=True, stdout=sys.stderr)
    print(binary.resolve())
