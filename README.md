# What is this?

CEP at Wimdu

This app listens to a mysql binlog stream and sends events to Esper so
they can be analyzed and queried.

Once a new Esper event is captured, it can be output to the console or
sent to graphite. More observers can be easily added to implement new
behaviors.

See [simao.io/gush-intro](http://simao.io/gush-intro)

# Running

Make sure scala and sbt are installed and in `PATH`.

1. `cp gush.config.yml.sample gush.config.yml`
2. Open `gush.config.yml` and adjust values
3. `sbt run`

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

## Writing more ESPER queries

There are unlimited possibilities here, writing queries to whatever happens in the database, real time.

For example:

- What is the CR of the last x seconds?

- How many bookings today?

## Implementing a subset of the statsd protocol

Would be nice to have gush receive UDP packets with the same format as
statsd.

If we send these events to Esper, we can then write queries that correlate statsd keys to database events, for example:

- What is the CR since the last deploy?

- Did the number of bookings increase since the last deploy?

## The deploy process is still a bit sketchy

We upload the sources to the server and compile there.

We can't assemble (sbt-assembly) a jar elsewhere and upload it because
the jar would be 20 MB. Maybe with with a decent internet connection
this would be OK, but right now it's not possible.

The current approach has a few problems, for example the `project` dir
is not synced on deploy, and this dir can hold important information
for the compilation.

## Remove hardcoded configuration values

Many configuration data is hardcoded into the code, machine hosts for
example. They should all be included in `gush.config.yml` and the app
should use those values.

## Write tests

Delete code and re write it test driven

## Other improvements

No patch is too small and all PR will be considered, just start doing
anything you would like to see implemented.

