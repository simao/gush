# What is this?

CEP at Wimdu

This app listens to a mysql binlog stream and sends events to ESPER so
they can be analyzed and queried.

Once a new ESPER event is captured, it can be output to the console or
sent to graphite. More observers can be easily added to implement new
behaviors.

See [simao.io/gush-intro](http://simao.io/gush-intro)

# Running

Make sure scala and sbt are installed and in `PATH`.

1. `cp gush.config.yml.sample gush.config.yml`
2. Open `gush.config.yml` and adjust values
3. Setup ssh tunnels if necessary: `ssh wimdu-reporting02 -L 9797:wimdu-db04:3306`
4. `sbt run`

# Deploying

You need python and fabric installed.

    fab -H <username>@<hostname> deploy

# Contributing

Patches are very welcome, even if they do not fix/implement anything
on the following ideas:

## Parsing updates

Currently `gush` only parses SQL updates. Would be nice to have UPDATE parsing

## Delete everything and write tests

Many code doesn't have tests. Would be a good idea to delete code and
re write it test driven.

## Upgrade scala and sbt

We are still using scala 2.10 with sbt 0.12.

## Improve deployment process

Currently we use `sbt assembly` but this generates a 20MB jar that we
need to upload to the server on each deploy.

Would be nice to write some chef recipes to install scala and sbt and
then the deploy script only needs to upload the new code. sbt can then
handle dependencies and compilation.

## Writing more ESPER queries

There are unlimited possibilities here, writing queries to whatever happens in the database, real time.

For example:

- What is the CR of the last x seconds?

- How many bookings today?

## Implementing a subset of the statsd protocol

Would be nice to have gush receive UDP packets with the same format as
statsd.

If we send these events to ESPER, we can then write queries that correlate statsd keys to database events, for example:

- What is the CR since the last deploy?

- Did the number of bookings increase since the last deploy?

## Use another logging library

scala-logging with a log4j backend is deprecated.

We need to migrate to scala-logging with a sl4j provider. I think
logback would be the best alternative as it's easier to migrate from
log4j2.

## The deploy process is still a bit sketchy

We upload the sources to the server and compile there.

We can't assemble (sbt-assembly) a jar elsewhere and upload it because
the jar would be 20 MB. Maybe with with a decent internet connection
this would be OK, but right now it's not possible.

The current approach has a few problems, for example the `project` dir
is not synced on deploy, and this dir can hold important information
for the compilation.

## Stop using `rx-scala`

This complicates things quite a bit without getting us anything in
Return, at least for the time being, it feels a bit over-engineered.

Stop using `rx-scala` and just use plain scala `Streams`.

We can also start using `akka` to manage the publish/subscribe nature
of the esper <-> gush communication. That would be simpler than using
`rx-scala` and probably more future proof.

## Other improvements

No patch is too small and all PR will be considered, just start doing
anything you would like to see implemented.
