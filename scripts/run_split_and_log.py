#!/usr/bin/env python3
import runpy
from pathlib import Path
import sys
from contextlib import redirect_stdout, redirect_stderr

LOG = Path(__file__).parent / 'split_run.log'
SCRIPT = Path(__file__).parent / 'split_kelmarsh_by_turbine.py'

with LOG.open('w', encoding='utf-8') as f:
    try:
        f.write(f'Running: {SCRIPT}\n')
        with redirect_stdout(f), redirect_stderr(f):
            runpy.run_path(str(SCRIPT), run_name='__main__')
    except SystemExit as e:
        f.write(f'SystemExit: {e}\n')
    except Exception as e:
        f.write('Exception during run:\n')
        import traceback
        traceback.print_exc(file=f)

print('Runner finished, log at:', LOG)

