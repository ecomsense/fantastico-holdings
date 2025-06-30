#!/bin/env sh

# Get the directory of the script and cd into it
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# Dynamically resolve full paths
TMUX=$(command -v tmux)
PYTHON=$(command -v python3)
GIT=$(command -v git)

# Fail if any command is missing
if [ -z "$TMUX" ] || [ -z "$PYTHON" ] || [ -z "$GIT" ]; then
  echo "Required command(s) not found in PATH. Exiting." >&2
  exit 1
fi

# Optional: change to your project directory
#cd /absolute/path/to/your/project || exit 1

sess="tmux-session"

if "$TMUX" has-session -t "$sess" 2>/dev/null; then
  echo "Session $sess already exists. Attaching to it."
  "$TMUX" attach -t "$sess"
else
  echo "updating"
  "$GIT" reset --hard && "$GIT" pull
  echo "Creating and attaching to session $sess."
  "$TMUX" new-session -d -s "$sess"
  "$TMUX" send-keys -t "$sess" "export PYTHONPATH=\$(pwd) && cd src && $PYTHON main.py && $TMUX kill-session -t $sess" C-m
  "$TMUX" attach -t "$sess"
fi
