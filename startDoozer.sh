rm ./doozer-0.5/dooz.log
touch ./doozer-0.5/dooz.log
./doozer-0.5/doozerd -l="localhost:8046" -w=":8080" >> ./doozer-0.5/dooz.log  2>&1 &
./doozer-0.5/doozerd -l="localhost:8047" -a="localhost:8046" -w=":8081" >> ./doozer-0.5/dooz.log  2>&1 &
./doozer-0.5/doozerd -l="localhost:8048" -a="localhost:8046" -w=":8082" >> ./doozer-0.5/dooz.log  2>&1 &