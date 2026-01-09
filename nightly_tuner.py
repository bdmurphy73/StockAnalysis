#!/usr/bin/env python3
"""
Run the optimizer nightly with locking, logging and basic record-keeping.

Designed to be run from systemd or cron. Uses the repository virtualenv
`venv_ai` by default; change `VENV_PY` if you use a different venv.
"""
import fcntl
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

LOCKFILE = Path('run/nightly_tuner.lock')
LOGFILE = Path('logs/nightly_tuner.log')
VENV_PY = Path('venv_ai/bin/python')
OPTIMIZER = Path('Optimizer.py')


def ensure_dirs():
    LOCKFILE.parent.mkdir(parents=True, exist_ok=True)
    LOGFILE.parent.mkdir(parents=True, exist_ok=True)


def acquire_lock(fd):
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False


def main(trials=100):
    ensure_dirs()
    logging.basicConfig(level=logging.INFO, filename=str(LOGFILE), filemode='a', format='%(asctime)s %(levelname)s %(message)s')
    logging.info('Nightly tuner starting; trials=%d', trials)
    with open(LOCKFILE, 'w') as lf:
        if not acquire_lock(lf):
            logging.info('Another instance is running; exiting')
            print('Another instance is running; exiting', file=sys.stderr)
            return 2
        # call Optimizer via venv python so environment is consistent
        cmd = [str(VENV_PY), str(OPTIMIZER), '--trials', str(trials)]
        logging.info('Running: %s', ' '.join(cmd))
        start = datetime.utcnow()
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            logging.info('Optimizer exit=%s', proc.returncode)
            logging.info('Optimizer stdout:\n%s', proc.stdout)
            logging.info('Optimizer stderr:\n%s', proc.stderr)
        except Exception as e:
            logging.exception('Optimizer run failed: %s', e)
        duration = (datetime.utcnow() - start).total_seconds()
        logging.info('Nightly tuner finished in %.1f seconds', duration)
    return 0


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--trials', type=int, default=100)
    args = p.parse_args()
    sys.exit(main(trials=args.trials))
