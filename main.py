import logging
import time
from queue import PriorityQueue
from queue import Queue

import cv2

from pooling import ThreadPool
from producer import Producer
from worker import Worker


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)-8s %(filename)s:%(lineno)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    datasource_queue = Queue(60)
    data_out_queue = PriorityQueue(60)
    try:
        main_producer = Producer.from_name("video", output_queue=datasource_queue)
        worker_base = Worker.from_name("video", input_queue=datasource_queue, output_queue=data_out_queue,
                                       use_multiprocess=True)

        thread_pool = ThreadPool()
        thread_pool.add_worker(main_producer, name='main_producer')
        thread_pool.add_worker(worker_base, nworkers=60)

        thread_pool.start()
        lens = 0

        while True:
            # time.sleep(0.01)
            out_data = data_out_queue.get()
            size = data_out_queue.qsize()
            print(
                "seq = %d, in size = %d, out size = %d, diff = %d" % (
                    out_data[0], datasource_queue.qsize(), size, size - lens
                )
            )
            lens = size
            cv2.imshow("preview", out_data[1])
            # cv2.imwrite('test/' + str(out_data[0]) + '.jpg', out_data[1])
            if cv2.waitKey(1) & 0xFF == ord('q'):
                thread_pool.stop()
                raise KeyboardInterrupt
    except KeyboardInterrupt as e:
        logging.info("App closing...")
        logging.info("Bye bye!")


if __name__ == '__main__':
    main()
