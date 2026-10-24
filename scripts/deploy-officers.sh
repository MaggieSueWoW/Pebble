#!/usr/bin/env bash
# Explicit target required; no dependency on the developer's active Firebase project.
set -euo pipefail
if [[ $# -ne 1 || -z "$1" ]]; then
  echo 'Usage: scripts/deploy-officers.sh FIREBASE_PROJECT_ID' >&2
  exit 2
fi
cd "$(dirname "$0")/.."
firebase deploy --project "$1" --only functions:officers,firestore:rules
firebase functions:artifacts:setpolicy --project "$1" --location us-central1 --days 1 --force
