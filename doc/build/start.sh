#!/bin/sh
set -e

# Note above that we run dumb-init as PID 1 in order to reap zombie processes
# as well as forward signals to all processes in its session. Normally, sh
# wouldn't do either of these functions so we'd leak zombies as well as do
# unclean termination of all our sub-processes.

# JOBPOOL_DATA_DIR is exposed as a volume for possible persistent storage. The
# JOBPOOL_CONFIG_DIR isn't exposed as a volume but you can compose additional
# cfg files in there if you use this image as a base, or use JOBPOOL_LOCAL_CONFIG
# below.
JOBPOOL_DATA_DIR=/jobpool/data
JOBPOOL_CONFIG_DIR=/etc/jobpool

# You can also set the JOBPOOL_LOCAL_CONFIG environment variable to pass some
# Jobpool configuration JSON without having to bind any volumes.
if [ -n "$JOBPOOL_LOCAL_CONFIG" ]; then
	echo "$JOBPOOL_LOCAL_CONFIG" > "$JOBPOOL_CONFIG_DIR/local.json"
fi

# If the user is trying to run Jobpool directly with some arguments, then
# pass them to Jobpool.
if [ "${1:0:1}" = '-' ]; then
    set -- jobpool "$@"
fi

# Look for Jobpool subcommands.
if [ "$1" = 'agent' ]; then
    shift
    set -- jobpool agent \
        -data-dir="$JOBPOOL_DATA_DIR" \
        -config=/etc/jobpool/config.json \
        "$@"
elif [ "$1" = 'version' ]; then
    # This needs a special case because there's no help output.
    set -- jobpool "$@"
elif jobpool --help "$1" 2>&1 | grep -q "jobpool $1"; then
    # We can't use the return code to check for the existence of a subcommand, so
    # we have to use grep to look for a pattern in the help output.
    set -- jobpool "$@"
fi

# If we are running Jobpool, make sure it executes as the proper user.
if [ "$1" = 'jobpool' ]; then
    # If the data or cfg dirs are bind mounted then chown them.
    # Note: This checks for root ownership as that's the most common case.
    if [ "$(stat -c %u /jobpool/data)" != "$(id -u root)" ]; then
        chown root:root /jobpool/data
    fi

    # If requested, set the capability to bind to privileged ports before
    # we drop to the non-root user. Note that this doesn't work with all
    # storage drivers (it won't work with AUFS).
    if [ ! -z ${JOBPOOL+x} ]; then
        setcap "cap_net_bind_service=+ep" /bin/jobpool
    fi

    set -- gosu root "$@"
fi
echo "$@"
exec "$@"
