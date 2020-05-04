for pid in `cat pids`; do
    kill $pid
done

rm pids
python gen_conf.py

for i in `seq 1 $QUORUM`; do
    echo "running $i"
    mkdir -p $i.dir
    ./main $i.json &
    echo $! >> pids
done
