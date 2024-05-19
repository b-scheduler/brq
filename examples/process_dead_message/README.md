## Process dead message

1. Run `python producer.py` to create a job
1. Run `python consumer.py` to process the message, it will fail and put the message to dead queue
1. Run `python process_dead_message.py` to process the dead message
