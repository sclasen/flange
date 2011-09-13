# Flange: Scala client For Doozer

## Build and test

    ./startDoozer.sh
    sbt test
    ./stopDoozer.sh

## Basic Usage (explicit types for clarity)
    ./startDoozer.sh
    sbt console

    import com.heroku.doozer.flange.Flange
    import com.heroku.doozer.flange.{GetResponse,SetResponse,ErrorResponse,WatchNotification,WatchResponse}

    val client = new Flange("doozer:?ca=localhost:8046&ca=localhost:8047&ca=localhost:8048")

    //Set returns an Left(ErrorResponse(_,_)) if there is a failure
    val set:Either[ErrorResponse,SetResponse] = client.set("/foo", "bar".getBytes(), 0L)

    //Set succeeds or throws an exception
    val set2:SetResponse = client.set_!("/foofoo", "bar".getBytes(), 0L)

    //Get returns an Left(ErrorResponse(_,_)) if there is a failure
    val get:Either[ErrorResponse,GetResponse] = client.get("/foo")
    //Get succeeds or exception is thrown
    val get2:GetResponse = client.get_!("/foofoo")
    //Watch succeeds or exception is thrown
    val watch:WatchResponse = client.watch_!("/foofoo", get2.cas){
        notification:WatchNotification => System.out.println("/foofoo changed to "+new String(notification.value))
    }
    client.set_!("/foofoo", "newbar".getBytes(), get2.cas)
    //Should print notification of change


##Failover

The Flange client will by default will failover to each of the doozer servers specified in the doozerURI when an operation fails. The failing operation
is transparently retried. Once all of the doozer servers have failed the client is dead.

This behavior is pluggable. You simply need to supply a function of List[String] => Iterable[String] when constructing a Flange instance
that will be passed the list of doozerd hosts parsed from the doozer URI.

The Flange companion object defines 2 of these functions, eachDoozerOnceStrategy and retryForeverStrategy. The default is eachDoozerOnceStrategy

To use the retryForeverStrategy

    import com.heroku.doozer.flange.Flange
    import com.heroku.doozer.flange.Flange._

    val doozerUri = ...
    val flange = new Flange(doozerUri, retryForeverStrategy)


To use your own

    import com.heroku.doozer.flange.Flange

    val funk: List[String]=>Iterable[String] = {doozerds:List[String]=>...}
    val doozerUri = ...
    val flange = new Flange(doozerUri, funk)




