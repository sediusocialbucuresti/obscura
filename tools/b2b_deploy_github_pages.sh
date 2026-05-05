#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR="${REPO_DIR:-/root/obscura}"
SITE_DIR="${SITE_DIR:-$REPO_DIR/data/b2b/site}"
REMOTE="${REMOTE:-fork}"
BRANCH="${BRANCH:-gh-pages}"
WORKTREE="${WORKTREE:-/tmp/obscura-gh-pages}"
MESSAGE="${MESSAGE:-Deploy B2B directory site}"

if [[ ! -d "$SITE_DIR" ]]; then
  echo "missing site directory: $SITE_DIR" >&2
  exit 1
fi

rm -rf "$WORKTREE"
mkdir -p "$WORKTREE"
cd "$WORKTREE"
git init
git remote add "$REMOTE" "$(cd "$REPO_DIR" && git remote get-url "$REMOTE")"
git checkout -b "$BRANCH"
cp -a "$SITE_DIR"/. .
touch .nojekyll
git add .
git commit -m "$MESSAGE"
git push -f "$REMOTE" "$BRANCH"
