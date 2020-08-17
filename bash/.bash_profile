# Do nothing for non-interactive shells (e.g. when VS Code connects)
if [ ! -z "$PS1" ]; then
  # Start fish shell
  # Start bash with the NOFISH environment variable set to disable
  if [ -z "$NOFISH" ] && [ -f "$HOME/usr/bin/fish" ]; then
    ~/usr/bin/fish
    # Exit from bash immediately if fish exited successfully
    if [ $? -eq 0 ]; then
      exit
    fi
    echo 'Fish exited with an error, dropping to bash shell'
    export PS1="\n\[\033[38;5;1m\]\u\[$(tput sgr0)\]@\[\033[38;5;5m\]\h\[$(tput sgr0)\] \[\033[38;5;4m\]\w\[$(tput sgr0)\]\n> "
  fi
fi
