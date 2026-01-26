import os
import subprocess
import time
import socket
import sys
import pytest

SERVER_CMD = [sys.executable, "-m", "uvicorn", "tests.sse_test_app:app", "--host", "127.0.0.1", "--port", "9001"]


def wait_for_port(host: str, port: int, timeout: float = 5.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except Exception:
            time.sleep(0.1)
    return False


@pytest.fixture(scope="session")
def base_url():
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join([os.getcwd(), env.get("PYTHONPATH", "")])
    p = subprocess.Popen(SERVER_CMD, env=env)
    if not wait_for_port('127.0.0.1', 9001, timeout=5.0):
        time.sleep(1.0)
    yield "http://127.0.0.1:9001"
    # teardown
    if p.poll() is None:
        p.terminate()
        try:
            p.wait(timeout=3)
        except Exception:
            p.kill()

