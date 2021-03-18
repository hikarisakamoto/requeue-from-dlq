import logging
import sys

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
sqs = boto3.resource('sqs')
client = boto3.client('sqs')


def retrieve_dlq_messages(queue, max_number, wait_time):
    try:
        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
        )
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        logger.info("Successfully retrieved %d messages", len(messages))
        return messages


def get_queue(name):
    try:
        queue = sqs.get_queue_by_name(QueueName=name)
        logger.info("Queue '%s' retrieved with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue


def read_message(message):
    return message.body


def requeue_message(queue_url, message):
    try:
        response = client.send_message(
            QueueUrl=queue_url,
            MessageBody=message
        )
        if 'Fail' in response:
            msg_meta = response['Fail']
            logger.warning(
                "Failed to send: %s: %s",
                msg_meta['MessageId'],
                message
            )
    except ClientError as error:
        logger.exception("Send message failed to queue: %s", queue_url)
        raise error
    else:
        return response


def delete_dql_messages(queue, retrieved_messages):
    try:
        entries = [{
            'Id': str(ind),
            'ReceiptHandle': msg.receipt_handle
        } for ind, msg in enumerate(retrieved_messages)]

        response = queue.delete_messages(Entries=entries)

        if 'Successful' in response:
            for msg_meta in response['Successful']:
                logger.info("Deleted %s", retrieved_messages[int(msg_meta['Id'])].receipt_handle)
        if 'Fail' in response:
            for msg_meta in response['Fail']:
                logger.warning("Could not delete %s", retrieved_messages[int(msg_meta['Id'])].receipt_handle)

    except ClientError:
        logger.exception("Couldn't delete messages from queue %s", queue)
    else:
        return response


def start(queue_url, dlq_name):
    dead_letter_queue = get_queue(dlq_name)
    batch_size = 10
    wait_time = 1
    has_messages = True

    print('Working', sep=" ")

    while has_messages:
        retrieved_messages = retrieve_dlq_messages(dead_letter_queue, batch_size, wait_time)
        print(' .', end='')
        sys.stdout.flush()
        for message in retrieved_messages:
            unpacked_message = read_message(message)
            requeue_message(queue_url, unpacked_message)
        if retrieved_messages:
            delete_dql_messages(dead_letter_queue, retrieved_messages)
        else:
            has_messages = False

    print('All done!')


def main():
    go = input("Running this script will delete all messages in your DLQ\n"
               "Run at your own risk!\n"
               "Do you want to continue (y/n)? \n")

    if go.lower() == 'y':
        queue_url = input("Please insert QUEUE URL")
        dlq_name = input("Please insert DEAD LETTER QUEUE (DLQ) -- NAME --")
        print("Starting to process messages!")
        start(queue_url, dlq_name)
    else:
        print("Cya!")


if __name__ == '__main__':
    main()
