# Gush

[![Build Status](https://travis-ci.org/simao/gush.png?branch=master)]https://travis-ci.org/simao/gush

Gush transforms your mysql database in a stream of data.

Gush connects to a mysql master and generates an streams of
events. This stream can be split, merged and transform using the
normal akka streams combinators. This way you can analyze your data as
a real time event stream. You can then threat your mysql data as
queriable real time stream.

# Examples

Gush's allows you to write queries like:

```scala
    def newBookingCount(): Sink[BinlogEvent, Future[Done]] = {
      val f = InsertEventFlow("bookings")
      val sink = Sink.foreach[BinlogInsertEvent](e ⇒ logger.info(s"New ${e.tableName} received"))
          f.toMat(sink)(Keep.right)
    }
```

```scala
  def bookingsWindowAvgRev(): Sink[BinlogEvent, Future[Done]] = {
      WindowedInsertAvg("bookings", "revenue", 10 seconds)(avg ⇒ logger.info(s"Average of last hour for bookings.avg is $avg"))
  }
```

# Running

Check the package `io.simao.gush_example` to see an example on how to use gush. 

# Unsupported features

- No support for `ON DUPLICATE KEY` statements. No plans to support this. Patches welcome!

# Contributing

Gush written in Scala and uses Akka Streams.

No pull request is too small, documentation improvements also very
welcome!

# Author

- Simão Mata - [simao.io](https://simao.io)
