from psycopg2 import connect
from csv import reader
from global_setup import *
import datetime
import threading
import argparse
import statistics
import os


class MyThread(threading.Thread):
    def __init__(self, thread_id, thread_name, hosts_queue, conn):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.thread_name = thread_name
        self.hosts_queue = hosts_queue
        self.conn = conn
        self.result = []

    def get_result(self):
        return self.result

    def run(self):
        cursor = self.conn.cursor()
        while self.hosts_queue:
            row = self.hosts_queue.pop(0)
            host = row[0]
            start_time = row[1]
            end_time = row[2]

            query_start_time = datetime.datetime.now()

            # Query execution with time bucket of 1 minute
            cursor.execute("SELECT time_bucket('1 minute', ts) as one_minute, host, min(usage), max(usage)\
            from cpu_usage \
            where host = %s and ts > %s and ts < %s\
            group by one_minute, host \
            order by one_minute, host", (host, start_time, end_time))

            query_end_time = datetime.datetime.now()

            # Total execution time for query
            total_time = (query_end_time - query_start_time).microseconds / 1000

            self.result.append(total_time)

            # print the result of query
            # for i, record in enumerate(cursor):
            #     print(record)

        # close the cursor object to avoid memory leaks
        cursor.close()


def main():
    conn = connect(
        dbname=DB_NAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD)

    args = parser.parse_args()

    # Exit if no input file provided
    if args.file is None:
        print("No valid input file provided. Exiting Program.......")
        exit()
    if not os.path.exists(args.file):
        print("No valid input file provided. Exiting Program........")
        exit()

    print("Processing with ", args.workers, " workers")

    no_of_threads = args.workers

    threads = []
    myDict = {}
    total_queries = 0

    # read query_params.csv from command line argument
    with open(args.file, 'r') as read_obj:
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        # check if file is empty
        if header != None:
            # Calculate hash for each host to calculate the target thread
            for row in csv_reader:
                host_name = row[0]
                hash_value = hash(host_name)
                target_thread = hash_value % no_of_threads

                # Dictionary with thread number as key and list of input rows as value
                if target_thread not in myDict:
                    myDict[target_thread] = [row]

                else:
                    q = myDict[target_thread]
                    q.append(row)

                total_queries = total_queries + 1

    # Starting concurrent workers
    for x in range(no_of_threads):
        thread = MyThread(x, "Thread-" + str(x + 1), myDict[x], conn)
        thread.start()
        threads.append(thread)

    results = []
    total_time = 0

    # Wait for all threads to complete
    for t in threads:
        t.join()
        results = results + t.get_result()

    for ele in range(0, len(results)):
        total_time = total_time + results[ele]

    print("# of queries processed: " + str(total_queries))
    print("Total processing time across all queries: " + str(total_time) + " ms")
    print("Minimum time taken by a query: " + str(min(results)) + " ms")
    print("Median of query time: " + str(statistics.median(results)) + " ms")
    print("Average time taken by a query: " + str(total_time/total_queries) + " ms")
    print("Maximum time taken by a query: " + str(max(results)) + " ms")

    # close the connection as well
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--workers", help="No of workers", default=1, type=int)
    parser.add_argument("-f", "--file", help="path to input file")
    main()
