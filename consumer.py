
import argparse
import time

from confluent_kafka import Consumer


def main():
    # Read the Kafka topic, output file, and consumer settings.
    parser = argparse.ArgumentParser(description='Write a Kafka topic to a file')
    parser.add_argument('--topic', default='file-topic')
    parser.add_argument('--out', default='received.txt')
    parser.add_argument('--bootstrap', default='localhost:9092')
    parser.add_argument('--idle', type=int, default=10,
                        help='Stop after this many seconds without messages')
    parser.add_argument('--group', default=f'file-consumer-{int(time.time())}')
    args = parser.parse_args()

    # Connect to Kafka and read from the beginning for a new consumer group.
    consumer = Consumer({
        'bootstrap.servers': args.bootstrap,
        'group.id': args.group,
        'auto.offset.reset': 'earliest',
    })

    consumer.subscribe([args.topic])

    count = 0
    last_message = time.time()

    try:
        # Rebuild the text file from messages received from Kafka.
        with open(args.out, 'w', encoding='utf-8', newline='\n') as out:
            while True:
                msg = consumer.poll(1.0)

                # Stop when no new messages arrive within the idle period.
                if msg is None:
                    if time.time() - last_message > args.idle:
                        break
                    continue

                # Report Kafka consumer errors without writing them to the file.
                if msg.error():
                    print(f'Consumer error: {msg.error()}')
                    continue

                # Decode each message and write it as one line.
                out.write(msg.value().decode('utf-8') + '\n')
                count += 1
                last_message = time.time()

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer cleanly.
        consumer.close()

    print(f'Wrote {count} lines to {args.out}')


if __name__ == '__main__':
    main()
