"""Real large selection must not overflow argv or hide tracked changes/errors."""
from pathlib import Path
import os
import subprocess
import tempfile
import unittest

HELPER = Path(__file__).resolve().parents[1] / "lib/architecture_guard/diff_cache.sh"


class DiffCapacity(unittest.TestCase):
    def test_large_untracked_selection_keeps_complete_diff_and_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", directory], check=True)
            (root / "tracked.rs").write_text("before\n")
            subprocess.run(["git", "add", "tracked.rs"], cwd=root, check=True)
            (root / "tracked.rs").write_text("after\n")
            # This fixture verifies local staged/unstaged diffs in its own repository.
            # Runner CI mode/range belong to the outer checkout, not this fixture.
            env = dict(os.environ)
            for name in ("GITHUB_ACTIONS", "ARCH_GUARD_DIFF_RANGE"):
                env.pop(name, None)
            script = '''set -e
source "$1"
files=()
for ((i=0;i<40000;i++)); do files+=("audit/untracked-evidence-$i-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"); done
prepare_diff_cache
cat "$architecture_diff_dir/main-0"
cat "$architecture_diff_dir/cached-0"
'''
            result = subprocess.run(["bash", "-c", script, "bash", str(HELPER)], cwd=root,
                                    text=True, capture_output=True, timeout=30, env=env)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("+after", result.stdout)
            self.assertIn("+before", result.stdout)
            result = subprocess.run(["bash", "-c", 'set -e; source "$1"; ARCH_GUARD_DIFF_RANGE=missing..bad; prepare_diff_cache',
                                     "bash", str(HELPER)], cwd=root, text=True, capture_output=True, timeout=30, env=env)
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
