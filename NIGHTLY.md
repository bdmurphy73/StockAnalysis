# Nightly Tuning

Automated nightly tuning uses `Optimizer.py` to run randomized parameter search and record the best configuration in the database.

Files added:
- `nightly_tuner.py` — wrapper that acquires a lock, runs the optimizer via the project's `venv_ai` and logs output to `logs/nightly_tuner.log`.
- `deploy/stock_tuner.service` and `deploy/stock_tuner.timer` — example systemd unit and timer (edit paths and user before enabling).

Systemd example (edit paths and user):
```bash
sudo cp deploy/stock_tuner.service /etc/systemd/system/stock_tuner.service
sudo cp deploy/stock_tuner.timer /etc/systemd/system/stock_tuner.timer
sudo systemctl daemon-reload
sudo systemctl enable --now stock_tuner.timer
```

Cron example to run at 02:00 daily:
```cron
0 2 * * * cd /home/PyFin/Documents/StockAnalysis/StockAnalysis && ./venv_ai/bin/python nightly_tuner.py --trials 100
```

Logs are written to `logs/nightly_tuner.log` and a lockfile is created at `run/nightly_tuner.lock` while the job runs.
