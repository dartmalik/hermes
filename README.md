# INTRODUCTION
This repository implements Hermes which is an overlay network that routes messages from senders to receivers. Hermes is inspired from IP routing and actor systems. However, Hermes does not implement any strict structure on the application. Receivers are simple functions that receive messages from the network. Hermes enables (or aims to enables) the following approaches to architecting applications:
* Structure the applications using a traditional (go) paradigm and use Hermes as a communications and routing layer (similar to IP).
* Structure the application using an actor-like architecture.
* A hybrid of the above two approaches.

# WHY HERMES?
Currently, Hermes does not support clustering and can be used to implement concurrent applications. Hermes provides the following advantages without support for clustering:
* Hermes manages the lifecycle of receivers i.e. receivers are instantiated (via the provided receiver factory), activated on message receipt and deactivated when idle. Closing hermes will also deactivates all receivers.
* The number of goroutines is limited to the number active receivers not the total receivers in the application. That is, the number of goroutines are equivalent to the number of messages being process at any time.
* Receivers can be tested independently as receivers communicate only via messages and dependent receivers can be mocked.
* Receivers can set other functions as receivers while processing messages. This allows entities to be in different states.
* Receivers execute one messages at a time which can reduce contention in some domains (collaborative domains).
* Receivers can be backed by durable state (from a database for instance). This allows state to be cached and allows the system to scale.
* Since state is cached on the application, low-latency workload can be supported.
* Supports timed message delivery for handling things like heartbeats, timeouts, etc

After clustering has been supported, Hermes will support a single abstraction for implementing both concurrent and distributed applications. This approach has many production usecases (in many industries) for implementing systems that can not be implement using conventional frameworks.

# GETTING STARTED
For getting started with Hermes, take a look at the tests and the code documentation. The apps/mqtt directory implements a MQTT 3.1.1 compatible broker using Hermes. This
