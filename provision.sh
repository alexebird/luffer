#!/usr/bin/env bash
set -euo pipefail

HOST="root@104.248.223.186"

rsync -a --progress --exclude target . "${HOST}":/root/luffer/
gpg -q -d ../pts/davinci/env/prod/luffer.env.gpg | ssh "${HOST}" 'cat - > /root/luffer.env'
ssh "${HOST}" bash < provision_remote.sh
