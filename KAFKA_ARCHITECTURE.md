\# Apache Kafka — Full Architecture and Explanation



\*\*Data Engineering | Week 8 — Lecture 1\*\*



\## 1. Introduction to Apache Kafka



Apache Kafka is a distributed event-streaming platform used to move and store data between different applications.



In Data Engineering, Kafka helps applications exchange information without requiring every system to communicate directly with every other system.



For example, when a customer places an order, the order information can be sent to Kafka. Other applications can then read that information for processing, analytics, or storage.



\*\*My understanding:\*\* Kafka acts as a central platform where applications publish events and other applications consume them. This makes data pipelines more flexible and scalable.



\## 2. Why Apache Kafka Is Needed



Without Kafka, different applications may need separate connections to exchange data.



As more applications are added, managing these connections becomes increasingly difficult.



Kafka solves this problem by providing a shared event stream that multiple applications can use.



\*\*My understanding:\*\* Instead of building a separate connection between every application, we can use Kafka as a common communication platform.



\## 3. Kafka Architecture Overview



The basic Kafka data flow is:



```text

Data Source

&#x20;    |

&#x20;    v

&#x20; Producer

&#x20;    |

&#x20;    v

Kafka Cluster

&#x20;    |

&#x20;    v

&#x20;   Topic

&#x20;    |

&#x20;    v

&#x20; Partitions

&#x20;    |

&#x20;    v

&#x20; Consumer

&#x20;    |

&#x20;    v

Destination System

```



The main architectural components are:



\- Producer

\- Event or message

\- Topic

\- Partition

\- Broker

\- Kafka cluster

\- Replication

\- Consumer

\- Consumer group

\- Offset

\- KRaft controllers



These components work together to publish, store, distribute, and process events.



\## 4. Producers



A producer is an application that sends events to Kafka topics.



A producer can send information such as customer orders, website activities, sensor readings, or text messages.



Producers can use batching to improve throughput, retries to handle temporary failures, and acknowledgments to control how Kafka confirms writes.



\*\*My practical experience:\*\* In Kafka Lab 2, I created `producer.py` using the Python `confluent-kafka` library.



My producer:



1\. Opened `sample.txt`.

2\. Read the file line by line.

3\. Converted each line into a Kafka message.

4\. Sent the messages to `file-topic`.

5\. Waited for message delivery to finish.



The program successfully reported that five out of five lines were sent.



\## 5. Events and Topics



An event represents something that happened.



A Kafka event can contain a key, value, timestamp, and optional headers.



A topic is a named stream of events.



For example, an online shopping platform might use separate topics for orders, payments, and deliveries.



\*\*My understanding:\*\* A topic organizes related events so producers know where to send them and consumers know where to read them.



\*\*My practical experience:\*\* I created a topic named `file-topic` to transfer text messages between my Python programs.



\## 6. Partitions and Message Ordering



A Kafka topic is divided into partitions.



Partitions allow Kafka to distribute data and process messages in parallel.



Kafka stores events in an ordered sequence within each partition.



However, Kafka does not guarantee one global order across all partitions of a topic.



\*\*My understanding:\*\* Partitions help Kafka handle larger workloads. Message ordering is maintained within an individual partition.



In my lab, the topic had one partition, which allowed the file's lines to remain in order.



\## 7. Message Keys



Kafka producers can assign keys to messages.



The key influences which partition receives a message.



Messages with the same key are normally routed to the same partition, allowing their relative order to be preserved.



In the lecture, the NorthStar Grocers example uses a store ID as the message key to maintain ordering for each store.



\*\*My practical experience:\*\* My producer used the input filename as the message key. This helped keep the lines of the same file together.



\## 8. Brokers and Kafka Clusters



A Kafka broker is a server that stores partitions and serves requests from producers and consumers.



A Kafka cluster consists of one or more brokers.



In a multi-broker cluster, partitions can be distributed across different brokers to share the workload.



\*\*My understanding:\*\* Brokers are the servers responsible for storing Kafka events. A cluster combines brokers to support larger workloads and improve availability.



\*\*My practical experience:\*\* I used a single Kafka broker running inside a Docker container named `kafka`.



\## 9. Replication, Leaders, and Followers



Kafka supports replication to protect data against broker failures.



A partition can have multiple copies called replicas.



One replica acts as the leader, while follower replicas copy its data.



The replication factor determines how many copies of a partition are maintained.



Kafka also tracks in-sync replicas (ISR), which are replicas that are sufficiently caught up with the leader.



\*\*My understanding:\*\* Replication improves fault tolerance. If a broker fails, an eligible replica can take over leadership.



My local lab used one broker and a replication factor of one, so it did not demonstrate multi-broker fault tolerance.



\## 10. KRaft and Cluster Coordination



Kafka requires coordination to manage cluster metadata and leadership.



Older Kafka architectures used Apache ZooKeeper for coordination.



Modern Kafka uses KRaft, where Kafka controllers manage metadata and leader election through a built-in controller quorum.



\*\*My understanding:\*\* KRaft simplifies Kafka architecture by reducing the need for a separate coordination system.



\## 11. Consumers



A consumer is an application that reads events from Kafka topics.



Consumers can process events, save them to files, update databases, or send them to other systems.



\*\*My practical experience:\*\* I created `consumer.py` using the `confluent-kafka` library.



My consumer:



1\. Connected to the Kafka broker.

2\. Subscribed to `file-topic`.

3\. Read the messages.

4\. Converted the message values into text.

5\. Wrote the lines into `received.txt`.



The consumer successfully reconstructed the original five-line file.



\## 12. Consumer Groups



A consumer group is a collection of consumers working together to read a topic.



Within a group, each partition is assigned to one consumer at a time.



This allows consumers to share processing work.



Different consumer groups can read the same topic independently.



\*\*My understanding:\*\* Consumer groups make it possible to scale message processing while avoiding multiple consumers in the same group processing the same partition simultaneously.



In my lab, the consumer used a new group ID for each run, allowing it to read earlier messages again.



\## 13. Offsets and Rebalancing



Every event within a Kafka partition has an offset.



An offset identifies the event's position in that partition.



Consumers use offsets to track their reading progress.



A consumer group can commit offsets so it can resume after restarting.



When consumers join or leave a group, Kafka may redistribute partition assignments. This process is called rebalancing.



\*\*My understanding:\*\* Offsets work like position markers. They help consumers continue processing from a known point.



\## 14. Kafka as an Append-Only Log



Kafka stores events in an append-only log.



New events are added to the end of the log rather than modifying existing events.



Kafka retains events according to configured retention policies, commonly based on time or size.



Reading an event does not automatically delete it.



\*\*My practical experience:\*\* During the live test, my consumer could read previously stored messages and newly produced messages.



This demonstrated Kafka's ability to replay retained events.



\## 15. Producer Acknowledgments and Delivery Guarantees



Kafka provides different settings and mechanisms to control message delivery.



\### Producer acknowledgments



Acknowledgments, or `acks`, determine when the producer considers a write successful.



For stronger durability, `acks=all` requires acknowledgment according to Kafka's in-sync replica configuration.



\### Delivery semantics



\*\*At-most-once:\*\* A message may be lost, but processing avoids duplicates.



\*\*At-least-once:\*\* Messages are processed at least once, but duplicates may occur.



\*\*Exactly-once:\*\* Kafka supports exactly-once processing through mechanisms such as idempotent production and transactions, within the appropriate processing scope.



\*\*My understanding:\*\* There is a tradeoff between reliability, performance, and implementation complexity. Data Engineers must choose suitable delivery guarantees for their applications.



My lab demonstrated basic message transfer, not a full exactly-once processing implementation.



\## 16. Kafka in Data Engineering and Lakehouse Architecture



Kafka is commonly used as an ingestion layer in modern data platforms.



The lecture illustrates how Kafka can feed a lakehouse pipeline:



```text

Data Sources

&#x20;    |

&#x20;    v

&#x20;Apache Kafka

&#x20;    |

&#x20;    v

Bronze Layer

(Raw Data)

&#x20;    |

&#x20;    v

Silver Layer

(Cleaned Data)

&#x20;    |

&#x20;    v

Gold Layer

(Business Metrics)

&#x20;    |

&#x20;    v

Dashboards and Analytics

```



The Bronze layer stores raw incoming data.



The Silver layer cleans and enriches data.



The Gold layer provides aggregated, business-ready information.



\*\*My understanding:\*\* Kafka handles event streaming and ingestion, while the lakehouse layers organize data for long-term storage, processing, and analytics.



\## 17. My Kafka Lab 2 Implementation



For the practical assignment, I developed a basic Python file-transfer pipeline.



```text

sample.txt

&#x20;   |

&#x20;   v

producer.py

&#x20;   |

&#x20;   v

Kafka Broker (Docker)

&#x20;   |

&#x20;   v

Topic: file-topic

&#x20;   |

&#x20;   v

consumer.py

&#x20;   |

&#x20;   v

received.txt

```



\### Technologies



\- Apache Kafka

\- Docker Desktop

\- Python 3.12

\- confluent-kafka

\- Windows PowerShell



\### Results



The producer successfully sent five lines from `sample.txt` to Kafka.



The consumer successfully received the messages and reconstructed the file as `received.txt`.



I verified the files using PowerShell's `Compare-Object` command. No differences were reported.



For the live test, I used two PowerShell windows to run the consumer and producer separately. The resulting `live.txt` contained ten lines: five previously stored messages and five newly sent messages.



A connection timeout warning occurred during the live test, but the expected ten lines were present in the output file.



\## 18. Key Learning Outcomes



Through the lecture and practical exercises, I learned:



1\. How Kafka producers publish events to topics.

2\. How brokers store events in partitions.

3\. How message keys influence partition placement.

4\. Why replication is important for fault tolerance.

5\. How consumers and consumer groups read events.

6\. How offsets support recovery and replay.

7\. Why delivery guarantees matter.

8\. How Kafka can support a modern Data Engineering pipeline.



\## 19. Conclusion



Apache Kafka is an important technology for building scalable event-driven data pipelines.



Its architecture separates data producers from consumers while providing partitioned storage, event replay, and configurable reliability.



My practical implementation helped me connect the theoretical architecture to a working Python producer-consumer pipeline.



Although the lab used a single Kafka broker and a small text file, it demonstrated the core principles of event publishing, message consumption, file reconstruction, and verification.



\*\*Overall reflection:\*\* This assignment helped me understand how data moves between applications through Kafka and why Kafka is useful in larger Data Engineering systems.



\---



\*Based on Week 8 — Lecture 1: Apache Kafka Architecture Deep Dive, together with my completed Kafka Lab 2 practical exercise.\*

