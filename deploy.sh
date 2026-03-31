#!/bin/bash
set -e

VM_HOST="autotrader-vm"
REMOTE_DIR="~/cold-email-infra"
BRANCH=$(git rev-parse --abbrev-ref HEAD)

# 1. Check for uncommitted changes
if [ -n "$(git status --porcelain)" ]; then
    echo "ERROR: Uncommitted changes. Commit or stash before deploying."
    exit 1
fi

# 2. Push to GitHub
echo "Step 1: Pushing $BRANCH to GitHub..."
git push origin "$BRANCH"

# 3. Pull on VM and restart
echo "Step 2: Pulling on VM and restarting service..."
ssh $VM_HOST "cd $REMOTE_DIR && git pull origin $BRANCH && bash restart.sh"

echo "Deploy complete. Check logs:"
echo "  ssh $VM_HOST 'tail -50 $REMOTE_DIR/leadgen.log'"
