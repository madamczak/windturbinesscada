"""
Recursively extract nested zip files into a single flat directory.

Usage:
    python scripts/extract_nested_zip.py

Defaults:
    source: data/zip/16807551.zip
    out_dir: data/kelmarsh_data_2

Behavior:
- Creates the output directory if needed
- Opens the top-level zip and extracts all non-zip files directly into out_dir
- For zip members that are zip files, reads them into memory and processes them recursively
- Flattens directory structure: every extracted file is written to out_dir/<basename>
- Overwrites files with the same name (prints a warning when overwriting)

This keeps memory usage reasonable by streaming where possible; nested zip bytes are read into memory briefly.
"""
from pathlib import Path
import zipfile
import io
import sys

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ZIP = ROOT / 'data' / 'zip' / '16807551.zip'
OUT_DIR = ROOT / 'data' / 'kelmarsh_data_2'


def safe_write_file(out_dir: Path, member_name: str, data: bytes):
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / Path(member_name).name
    if target.exists():
        print(f"Overwriting existing file: {target.name}")
    with open(target, 'wb') as f:
        f.write(data)


def process_zip_bytes(zip_bytes: bytes, out_dir: Path, parent_path: str = '') -> int:
    """Process a zip stored in bytes, extract files into out_dir. Returns number of files extracted."""
    extracted = 0
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for info in z.infolist():
            name = info.filename
            # skip directories
            if info.is_dir():
                continue
            # read the file bytes
            try:
                with z.open(info) as member:
                    data = member.read()
            except RuntimeError:
                # fallback: skip problematic member
                print(f"Warning: failed to read member {name}; skipping")
                continue
            lower = name.lower()
            if lower.endswith('.zip'):
                # recurse into nested zip
                print(f"Found nested zip: {parent_path + name}; processing...")
                try:
                    extracted += process_zip_bytes(data, out_dir, parent_path=parent_path + name + '::')
                except Exception as e:
                    print(f"Error processing nested zip {name}: {e}")
            else:
                # write flattened file
                safe_write_file(out_dir, name, data)
                extracted += 1
    return extracted


def main(source_zip: Path = None):
    source = Path(source_zip) if source_zip else DEFAULT_ZIP
    if not source.exists():
        print(f"Source zip not found: {source}")
        return 2
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    total = 0
    print(f"Extracting nested zips from {source} into {OUT_DIR} (flattened)")
    with zipfile.ZipFile(source) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            name = info.filename
            lower = name.lower()
            with z.open(info) as member:
                data = member.read()
            if lower.endswith('.zip'):
                print(f"Processing nested zip member: {name}")
                try:
                    total += process_zip_bytes(data, OUT_DIR, parent_path=name + '::')
                except Exception as e:
                    print(f"Failed to process nested zip {name}: {e}")
            else:
                safe_write_file(OUT_DIR, name, data)
                total += 1
    print(f"Done. Extracted {total} files into {OUT_DIR}")
    return 0


if __name__ == '__main__':
    src = None
    if len(sys.argv) > 1:
        src = sys.argv[1]
    raise SystemExit(main(src))

