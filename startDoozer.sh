rm ./doozer-0.6/dooz.log

set -e
trap quit INT TERM EXIT

quit() {
    killall doozerd
    echo "Unable to successfully start the cluster"
    exit
}

touch ./doozer-0.6/dooz.log

./doozer-0.6/doozerd -l="127.0.0.1:8046" -w=":8080" >> ./doozer-0.6/dooz.log  2>&1 &
./doozer-0.6/doozerd -l="127.0.0.1:8047" -a="127.0.0.1:8046" -w=":8081" >> ./doozer-0.6/dooz.log  2>&1 &
./doozer-0.6/doozerd -l="127.0.0.1:8048" -a="127.0.0.1:8046" -w=":8082" >> ./doozer-0.6/dooz.log  2>&1 &

sleep 1

printf '' | ./doozer-0.6/doozer -a 127.0.0.1:8046 set /ctl/cal/1 0  > /dev/null
printf '' | ./doozer-0.6/doozer -a 127.0.0.1:8046 set /ctl/cal/2 0  > /dev/null


trap - INT TERM EXIT
echo "Started"


