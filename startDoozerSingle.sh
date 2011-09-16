rm ./doozerd-0.8/dooz.log

set -e
trap quit INT TERM EXIT

quit() {
    killall doozerd
    echo "Unable to successfully start the cluster"
    exit
}

export DOOZER_RWSECRET=secret

touch ./doozerd-0.8/dooz.log

OS=linux
UNAME=`uname`
if [ "$UNAME" =  "Darwin" ]; then
  OS=osx
fi

./doozerd-0.8/$OS-386/doozerd -l="127.0.0.1:8046" -w=":8080" >> ./doozerd-0.8/dooz.log  2>&1 &

sleep 1


trap - INT TERM EXIT
echo "Started"


