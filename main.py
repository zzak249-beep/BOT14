import runpy
import sys
import os

sys.path.insert(0, "/app")
os.chdir("/app")
runpy.run_path("/app/scanner.py", run_name="__main__")
