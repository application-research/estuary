#!/bin/sh

ulimit -n 100000

test_dir="/var/tmp/artest"
rm -rf $test_dir
mkdir -p $test_dir

export STORETHEINDEX_PATH="$test_dir/storetheindex"
export AUTORETRIEVE_DATA_DIR="$test_dir/autoretrieve"
export ESTUARY_DATADIR="$test_dir/estuary"

# set up storetheindex
storetheindex init
tmux new-session -d " \
    STORETHEINDEX_PATH=$STORETHEINDEX_PATH \
    storetheindex daemon \
"

# set up estuary
mkdir -p $ESTUARY_DATADIR
api_key=$(cd $ESTUARY_DATADIR && estuary setup --username u --password p | grep -Eo "EST.+ARY")
echo "Got API key: $api_key"
tmux split-window -c $ESTUARY_DATADIR -h " \
    cd $ESTUARY_DATADIR && \
    GOLOG_LOG_LEVEL='estuary=debug' \
    estuary \
    --indexer-url=http://127.0.0.1:3001 \
    --indexer-advertisement-interval=15s \
    --log-level='debug' \
"

# set up autoretrieve
mkdir -p $AUTORETRIEVE_DATA_DIR
autoretrieve gen-config
sleep 1 && autoretrieve register-estuary http://localhost:3004 $api_key
tmux split-window -h " \
    GOLOG_LOG_LEVEL='autoretrieve=debug' \
    autoretrieve \
    --data-dir=$AUTORETRIEVE_DATA_DIR \
    --disable-retrieval
"

# print out the example command in the scratch pane
tmux split-window
tmux send-keys " \
    curl --location --request POST 'http://localhost:3004/content/add' \
    --header 'Authorization: Bearer $api_key' \
    --form 'data=@\"$ESTUARY_DATADIR/estuary.db\"' \
"

tmux select-layout even-horizontal
tmux attach