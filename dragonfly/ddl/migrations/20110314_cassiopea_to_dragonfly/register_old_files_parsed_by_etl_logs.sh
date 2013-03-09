#!/bin/bash
PAN=/usr/local/pentaho/pdi/pan.sh
ROOT_DIR=/home/etl/
HOUR_DIFF=12

# The hour diff variables represents how recent should files be. Meaning files that were rotated by the log rotate but were not parsed by the etl_logs.sh

while getopts "p:h:d:" o
do	case "$o" in
    p)	PAN="$OPTARG";;
    h)	ROOT_DIR="$OPTARG";;
    d)  HOUR_DIFF="$OPTARG";;
	[?])	echo >&2 "Usage: $0 [-p  pdi-pan-path] [-h dwh-path] [-d hour-difference]"
		exit 1;;
	esac
done

export KETTLE_HOME=$ROOT_DIR
sh $PAN /file `pwd`/register_old_files_in_db.ktr -param:hourDiff=$HOUR_DIFF
exit $?
