#!/bin/bash
ARGS=""
if [ ! -z "$ES" ]; then ARGS+=" --host $ES"; fi
if [ ! -z "$PORT" ]; then ARGS+=" --port $PORT"; fi
if [ ! -z "$BATCH" ]; then ARGS+=" -b $BATCH"; fi
if [ ! -z "$FIELD" ]; then ARGS+=" -f $FIELD"; fi
if [ ! -z "$FLUSH" ]; then ARGS+=" --flush $FLUSH"; fi
if [ ! -z "$INDEX" ]; then ARGS+=" -i $INDEX"; fi
if [ ! -z "$DOC_TYPE" ]; then ARGS+=" --type $DOC_TYPE"; fi
if [ ! -z "$PREFIX" ]; then ARGS+=" --prefix $PREFIX"; fi
if [ ! -z "$PREFIX_SEP" ]; then ARGS+=" -s $PREFIX_SEP"; fi
if [ ! -z "$DUPES" ]; then ARGS+=" -m $DUPES"; fi
if [ ! -z "$INC" ]; then ARGS+=" -I $INC"; fi
if [ ! -z "$SLEEP" ]; then ARGS+=" --sleep $SLEEP"; fi
if [ ! -z "$LOG_AGG" ]; then ARGS+=" --log_agg $LOG_AGG"; fi
if [ ! -z "$LOG_DONE" ]; then ARGS+=" --log_done $LOG_DONE"; fi
if [ "$VERBOSE" == true ]; then ARGS+=" --verbose"; fi
if [ "$DEBUG" == true ]; then ARGS+=" --debug"; fi
if [ "$NOOP" == true ]; then ARGS+=" --noop"; fi
if [ "$ALL" == true ]; then ARGS+=" --all"; fi
if [ "$NO_CHECK" == true ]; then ARGS+=" --no-check"; fi

cmd="python3 dedupe.py $ARGS $@"
echo "Running: ${cmd}"
exec ${cmd}
