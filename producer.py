
import argparse
import os

from confluent_kafka import Producer


def main():
    # Read the file path and optional Kafka settings from the command line.
    parser = argparse.ArgumentParser(description='Send a text file to Kafka')
    parser.add_argument('file', help='Path of the text file to send')
    parser.add_argument('--topic', default='file-topic')
    parser.add_argument('--bootstrap', default='localhost:9092')
    args = parser.parse_args()

    # Connect the producer to the Kafka broker.
    producer = Producer({'bootstrap.servers': args.bootstrap})

    # Use the file name as the message key to preserve partition ordering.
    key = os.path.basename(args.file)
    failed = 0

    # Track messages that Kafka could not deliver.
    def on_delivery(err, msg):
        nonlocal failed
        if err is not None:
            failed += 1
            print(f'Delivery failed: {err}')

    count = 0

    # Read the text file and send each line as one Kafka message.
    with open(args.file, 'r', encoding='utf-8') as f:
        for line in f:
            value = line.rstrip('\r\n')

            # Retry if the producer's local message queue is full.
            while True:
                try:
                    producer.produce(
                        args.topic,
                        key=key,
                        value=value,
                        callback=on_delivery
                    )
                    break
                except BufferError:
                    producer.poll(1)

            count += 1
            producer.poll(0)

    # Wait for all queued messages to finish delivery.
    producer.flush()

    # Display how many messages were sent successfully.
    print(
        f'Sent {count - failed} of {count} lines '
        f'from {args.file} to topic {args.topic}'
    )


if __name__ == '__main__':
    main()
