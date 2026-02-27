# Encoding Guard

This repository uses a multi-layer encoding guard to reduce mojibake risk.

## What is enforced

1. Editor defaults:
   - `.editorconfig`
   - `.vscode/settings.json`
2. Git normalization:
   - `.gitattributes`
3. Commit-time checks:
   - `.pre-commit-config.yaml`
   - `tools/check_encoding.py`
4. CI check:
   - `.github/workflows/encoding-check.yml`

## Local usage

Install pre-commit once:

```bash
pip install pre-commit
pre-commit install
```

Run checks manually:

```bash
# Check all tracked files for UTF-8 only
python tools/check_encoding.py --no-mojibake-check

# Check specific files with mojibake detection
python tools/check_encoding.py server/databaseHelper.py
```

Windows terminal UTF-8 setup (PowerShell):

```powershell
[Console]::InputEncoding = [System.Text.UTF8Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::UTF8
$OutputEncoding = [System.Text.UTF8Encoding]::UTF8
```

## Notes

- The mojibake detector is intentionally strict for changed files.
- Existing legacy mojibake should be fixed incrementally.
