\# Apache Kafka Lab 2 — Python Producer and Consumer



\## Project Overview



This project demonstrates how to transfer a text file through Apache Kafka using a custom Python producer and consumer.



The producer reads a text file line by line and sends each line as a separate Kafka message. The consumer reads those messages from a Kafka topic and reconstructs the text file.



\## Technologies Used



\- Apache Kafka (Docker)

\- Python 3.12

\- confluent-kafka Python library

\- Docker Desktop

\- Windows PowerShell



\## Project Files



| File | Description |

|---|---|

| `producer.py` | Reads a text file and sends its lines to Kafka |

| `consumer.py` | Reads Kafka messages and writes them to a text file |

| `sample.txt` | Original input file containing five lines |

| `received.txt` | Reconstructed file from the consumer |

| `live.txt` | Output from the live producer-consumer test |



\## How the Pipeline Works



1\. The producer reads `sample.txt`.

2\. Each line becomes a Kafka message.

3\. Messages are published to the `file-topic` topic.

4\. The consumer subscribes to the topic.

5\. The consumer writes received messages to `received.txt`.

6\. PowerShell compares the original and reconstructed files.



\## Running the Project



\*\*Prerequisites:\*\* Python 3.9 or newer, Docker, and a running Kafka broker accessible at `localhost:9092`. Create the Kafka topic `file-topic` before running the scripts.



Install the Python library:



```powershell

python -m pip install confluent-kafka

```



Run the producer:



```powershell

python producer.py sample.txt --topic file-topic

```



Run the consumer:



```powershell

python consumer.py --topic file-topic --out received.txt

```



Verify the file transfer:



```powershell

Compare-Object (Get-Content sample.txt) (Get-Content received.txt)

```



No output means both files contain the same lines in the same order.



\## Live Test



In the first PowerShell window, start the consumer:



```powershell

python consumer.py --topic file-topic --out live.txt --idle 60

```



While the consumer is running, use a second PowerShell window to send the file again:



```powershell

python producer.py sample.txt --topic file-topic

```



A new consumer group starts reading from the beginning of the topic. In the lab's two-batch example, the consumer receives ten messages.



\## Results



\- Successfully sent five lines using the Python producer.

\- Successfully reconstructed the original five-line text file.

\- Verified that the original and received files matched.

\- Completed the live test and verified ten lines in `live.txt`.



A Kafka connection timeout warning appeared during the live test, although the expected messages were received.



\## Key Learning Outcomes



This exercise helped me practice using Kafka topics, Python producers and consumers, consumer groups, message offsets, and basic file-transfer verification.



The project demonstrates how Kafka can act as an intermediary between applications that produce and consume data.

