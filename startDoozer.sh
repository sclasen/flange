rm ./doozer-0.7/dooz.log

set -e
trap quit INT TERM EXIT

quit() {
    killall doozerd
    echo "Unable to successfully start the cluster"
    exit
}

export DOOZER_RWSECRET=secret

touch ./doozer-0.7/dooz.log

OS=linux
UNAME=`uname`
if [ "$UNAME" =  "Darwin" ]; then
  OS=osx
fi

./doozerd-0.8/$OS-386/doozerd -l="127.0.0.1:8046" -w=":8080" >> ./doozer-0.7/dooz.log  2>&1 &
./doozerd-0.8/$OS-386/doozerd -l="127.0.0.1:8047" -a="127.0.0.1:8046" -w=":8081" >> ./doozer-0.7/dooz.log  2>&1 &
./doozerd-0.8/$OS-386/doozerd -l="127.0.0.1:8048" -a="127.0.0.1:8046" -w=":8082" >> ./doozer-0.7/dooz.log  2>&1 &

sleep 1

printf '' | ./doozerd-0.8/$OS-386/doozer -a "doozer:?ca=127.0.0.1:8046&sk=$DOOZER_RWSECRET" set /ctl/cal/1 0  > /dev/null
printf '' | ./doozerd-0.8/$OS-386/doozer -a "doozer:?ca=127.0.0.1:8046&sk=$DOOZER_RWSECRET" set /ctl/cal/2 0  > /dev/null


trap - INT TERM EXIT
echo "Started"


