import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

os.environ.setdefault("AWS_ACCESS_KEY", "AKIATESTKEY")
os.environ.setdefault("AWS_SECRET_KEY", "test/secret+key")
os.environ.setdefault("AWS_BUCKET_NAME", "test-bucket")
