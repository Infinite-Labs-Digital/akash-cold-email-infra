#!/bin/bash
set -e

cd "$(dirname "$0")"

# Activate venv if exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

pip install -r requirements.txt --quiet

# Restart systemd service
sudo systemctl restart cold-email-infra.service
echo "Service restarted. Status:"
sudo systemctl status cold-email-infra.service --no-pager
