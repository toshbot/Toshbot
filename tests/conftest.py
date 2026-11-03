"""Makes `from tests.helpers import ...` work from any working directory, even if __init__.py is missing."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
