# Guru Cue Search &amp; Recommendations AI Engine

This is the AI engine of the Guru Cue Search &amp; Recommendations. It runs
as a daemon and communicates with the Guru Cue Search &amp; Recommendations
REST API using RMI.

# Building the AI Engine
The minimum required JDK version to build the AI Engine with is 1.8.

Before you start building the AI Engine put into the `libs` directory these Guru
Cue Search &amp; Recommendations libraries:
* `database`,
* `data-provider-jdbc`,
* `data-provider-postgresql`.

Perform the build using [gradle](https://gradle.org/). If you don't have it
installed, you can use the gradle wrapper script `gradlew` (Linux and similar)
or `gradlew.bat` (Windows).

# Deploying the AI Engine
The AI Engine should be run using the [Apache Commons Daemon](http://commons.apache.org/proper/commons-daemon/).
The AI engine main class for running as a daemon is `com.gurucue.recommendations.RmiServer` and takes
the following parameters:
* the database URL in the form `jdbc:<subprotocol>:<subname>`
* the database username
* the database password
* the `id` from the `recommender` database entity that the AI engine instance
is known under.

There can be more instances of the AI Engine running, each with its own unique
`id`. Each instance must have its settings configured in the database in the
`recommender_setting` table.

To build AI models offline, use the `com.gurucue.recomendations.ModelBuilder`
main class, which is the configured main class of the built `jar`.
