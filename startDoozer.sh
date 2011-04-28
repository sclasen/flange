rm ./doozer-0.6/dooz.log
touch ./doozer-0.6/dooz.log
./doozer-0.6/doozerd -l="127.0.0.1:8046" -w=":8080" >> ./doozer-0.6/dooz.log  2>&1 &
./doozer-0.6/doozerd -l="127.0.0.1:8047" -a="127.0.0.1:8046" -w=":8081" >> ./doozer-0.6/dooz.log  2>&1 &
./doozer-0.6/doozerd -l="127.0.0.1:8048" -a="127.0.0.1:8046" -w=":8082" >> ./doozer-0.6/dooz.log  2>&1 &