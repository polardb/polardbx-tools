#!/bin/sh
threads=4;
src="";
dst="";
function usage(){
  echo "usage: sh polarx_cn_transfer_parallel.sh -s sql.log -t 4 -d xxx.json ";
}

exists_cnt=`ps -ef | grep 'polarx_cn_transfer_parallel.py' | grep -v grep | wc -l | awk '{print $1}'`
if [ $exists_cnt -eq "0" ];then
    echo "polarx_cn_transfer_parallel.py process is already started"
    exit 1;
fi;



OPTS=`getopt -o "s:d:t:" -a -n "$0" -- "$@"`
eval set -- "$OPTS"

if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi

while true; do
#    echo ""
#    echo $OPTS
#    echo $1
#    echo $2

   case "$1" in
      -s)
        src=$2
        shift 2
        ;;
      -d)
        dst=$2
        shift 2
        ;;
      -t)
        threads=$2
        shift 2
        ;;
      *)
        break
        ;;
  esac
done
if [ -z $src -o -z $dst ];
then
  echo "-s and -d is needed";
  exit 1;
fi;

cnt=0;
if [ -f "$src" ]; then
    cnt=$(wc -l $src|awk '{print $1}')
fi
shard_cnt=`expr $cnt / $threads`
shard_cnt=`expr $shard_cnt + 1`
rm -f .__polarx_cnt_shard_*
split -l $shard_cnt -d -a 4 $src .__polarx_cnt_shard_

for file in `ls .__polarx_cnt_shard_* | grep -v '.json'`;
do
    python polarx_cn_transfer.py -s $file -d $file".json" &
done

while true ;do
    ps_cnt=`ps -ef | grep 'polarx_cn_transfer.py' | grep -v grep | wc -l | awk '{print $1}'`
    if [ $ps_cnt -eq "0" ];then
        break;
    fi;
    sleep 1;
done

echo > $dst

for file in `ls .__polarx_cnt_shard_*.json`;
do
    cat $file >>$dst
done

rm -f .__polarx_cnt_shard_*







