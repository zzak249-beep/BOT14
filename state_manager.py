"""
state_manager.py

Simple JSON state persistence with an atomic write (write to a temp file in
the same directory, fsync, then os.replace). This avoids a half-written
state file if the process is killed mid-save - e.g. on a Railway redeploy
or restart.

Note: Railway's container filesystem is ephemeral across redeploys unless
STATE_FILE_PATH points at a mounted Volume. Even so, main.py always
reconciles against BingX's live position data on startup rather than
trusting this file blindly, so a lost state file degrades gracefully
instead of causing a duplicate/orphaned position.
"""

import json
import logging
import os
import tempfile

logger = logging.getLogger("state")


class StateManager:
    def __init__(self, path: str):
        self.path = path
        directory = os.path.dirname(os.path.abspath(path)) or "."
        os.makedirs(directory, exist_ok=True)

    def load(self) -> dict:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to load state file (%s), starting fresh: %s", self.path, e)
            return {}

    def save(self, state: dict):
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".state_", suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(state, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.path)  # atomic on POSIX filesystems
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
