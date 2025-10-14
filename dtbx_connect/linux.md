## No password on sudo
- No terminal `sudo visudo`
- Altere a linha `%sudo   ALL=(ALL:ALL) NOPASSWD: ALL`

## Update distro
- Update Ubuntu: `sudo -- sh -c 'apt-get update; apt-get upgrade -y; apt-get dist-upgrade -y; apt-get autoremove -y; apt-get autoclean -y'`

## Show branch
- `sudo nano ~/.bashrc` add at end:
```
    git_data() {

        if [ -d .git ]
        then
                git status 2> /dev/null | grep "working tree clean" &> /dev/null
                if [ $? -ne 0 ]; then STATUS="!"; else STATUS=""; fi
                echo -n " (`git branch 2>/dev/null | grep '^*' | colrm 1 2`$STATUS)"
        fi
}

export PS1="\u@\h \[\033[36m\]\w\[\033[91m\]\$(git_data) \[\033[00m\]$ "
```

## git
- `git config --global user.email "viniciusdvale@gmail.com"`
- `git config --global user.name "vinicius.vale"`