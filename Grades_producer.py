"""
    This program sends temperature reading data from a smoker to a queue on the RabbitMQ server. 

    Author: Amelia Teare
    Date: October 5, 2023

"""

import pika
import pandas as pd
import sys
import webbrowser
import csv
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

HOST = "localhost"
PORT = 9999
ADDRESS_TUPLE = (HOST, PORT)
FILE_NAME_TASKS = "Grades.csv"
SHOW_OFFER = True # By selecting True, you are ensuring that the admin website automatically opens. To turn this feature off, type "FALSE"

def rabbit_admin():
    """Offer to open the RabbitMQ Admin website"""
    global SHOW_OFFER
    if SHOW_OFFER:
        webbrowser.open_new("http://localhost:15672/#/queues")


def read_from_file(file_name):
    conn = pika.BlockingConnection(pika.ConnectionParameters(HOST))
    ch = conn.channel()
    class_names = ['Math', 'Science', 'English', 'Social Studies', 'Reading']
    
    with open(file_name, "r", newline="") as input_file:
            reader = csv.reader(input_file)
            next(reader, None)
                
            for row in reader:
                student_id = row[0]
                grades = [float(grade) for grade in row[1:]]

                grade_data = {class_names[i]: grades[i] for i in range(len(class_names))}

                ch.basic_publish(exchange="", routing_key='grade_data', body=str(grade_data))
                logging.info(f"Grade data has been sent to {'grade_data'} Queue")

                time.sleep(10)

                logging.info(" Type CTRL+C to cancel the program  ")
  

def main():
    """
    This function will be used to do the following:
    1. connect to RabbitMQ
    2. Get a communication channel
    3. Use the channel to queue_delete() the queue
    4. Use the channel to queue_declare() the queue
    5. Open the file, get your csv reader, for each row, use the channel to basic_publish() a message.
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(HOST))
        
        # use the connection to create a communication channel
        ch = conn.channel()

        # delete the existing queues using queue_delete()
        ch.queue_delete(queue="grade_data")

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue="grade_data", durable=True)
        

        read_from_file(FILE_NAME_TASKS)        

    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()



# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    rabbit_admin()
   
    # get the tasks from the csv file using the custom function
    main()