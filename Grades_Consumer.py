"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Author: Amelia Teare
    Date: October 5, 2023

"""

import pika
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

failing_threshold = 1.0 

# define a callback function to be called when a message is receive

def callback_message(ch, method, properties, body):
    grade_data = eval(body)
    student_id = grade_data.get('StudentID')

    for class_name, grade in grade_data.items():
        if class_name != 'StudentID' and grade < failing_threshold:
            logging.info(f"Failing grade alert for Student {student_id}: {class_name} grade is {grade: .2f}")


# define a main function to run the program
def main(hn: str = "localhost"):
    
    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

        # use the connection to create a communication channel
        channel = connection.channel()

        channel.queue_declare(queue="grade_data", durable=True)

        channel.basic_consume(queue="grade_data", on_message_callback=callback_message, auto_ack=False)
        

        # log a message for the user
        logging.info(" [*] Waiting for failing grade alerts. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logging.info("")
        logging.error("ERROR: Something went wrong.")
        logging.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("")
        logging.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logging.info("\nClosing connection. Goodbye.\n")
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")
