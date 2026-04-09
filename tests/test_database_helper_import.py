import os
import subprocess
import sys
import textwrap


def test_database_helper_import_keeps_dbbuild_versioning_importable():
    repo_root = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir))
    script = textwrap.dedent(
        f"""
        import os
        import sys

        repo_root = {repo_root!r}
        server_dir = os.path.join(repo_root, "server")
        dbbuild_dir = os.path.join(server_dir, "dbBuild")
        if server_dir not in sys.path:
            sys.path.insert(0, server_dir)

        import databaseHelper  # noqa: F401

        if dbbuild_dir not in sys.path:
            sys.path.insert(0, dbbuild_dir)

        import history_backfill  # noqa: F401
        import versioning

        assert versioning.VERSION_DIM_TABLE == "version_dim"
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout
