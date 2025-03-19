import random
import time
from dataclasses import dataclass
from queue import Queue
from threading import Lock, Thread, Condition
from typing import Optional

@dataclass
class Job:
    id: int

class IDGenerator:
    def __init__(self):
        self.last_id = 0
        self.lock = Lock()
    
    def generate(self) -> int:
        with self.lock:
            job_id = self.last_id
            self.last_id += 1
            return job_id

class Executor:
    def __init__(self):
        self.total_sleep = 0
        self.total_jobs = 0
        self.lock = Lock()
        
    def execute_job(self, job: Job) -> Job:
        # Sleep between 300ms and 1100ms
        sleep_duration = random.randint(300, 1100) / 1000
        time.sleep(sleep_duration)
        
        with self.lock:
            self.total_sleep += sleep_duration * 1000  # Convert to ms
            self.total_jobs += 1
        
        return job
    
    def get_avg_sleep(self) -> float:
        if self.total_jobs == 0:
            return 0
        return self.total_sleep / self.total_jobs

class Aggregator:
    def __init__(self):
        self.last_id = -1
        self.lock = Lock()
        self.result_queue = Queue()
        
    def try_add_job(self, job: Job) -> tuple[int, Optional[str]]:
        with self.lock:
            if job.id == -100:
                self.result_queue.put(None)
                return 0, None
            if job.id == self.last_id + 1:
                self.last_id = job.id
                self.result_queue.put(job)
                return 0, None
            return (job.id - self.last_id), "Job ID is out of order"
            
    def flush_results(self):
        print("flushing results")
        while True:
            job = self.result_queue.get()  # Blocking call
            if job is None:
                break
        print(f"Flushed {self.last_id} jobs")

class Producer:
    def __init__(self, limit: int):
        self.limit = limit
        self.id_generator = IDGenerator()
        self.job_queue = Queue()
        
    def produce(self):
        print("started producing jobs")
        start_time = time.time()
        for _ in range(self.limit):
            time.sleep(random.randint(0, 50) / 1000)  # 0-50ms sleep
            job = Job(id=self.id_generator.generate())
            self.job_queue.put(job)
        self.job_queue.put(None)  # Sentinel value
        print(f"Time taken to produce: {time.time() - start_time:.2f} seconds")

def process_jobs(job_queue: Queue, aggregator: Aggregator, executor: Executor, needs_backoff: bool):
    active_threads = 0
    thread_lock = Lock()
    thread_done = Condition(thread_lock)
    print("started processing jobs")
    while True:
        job = job_queue.get()
        if job is None:  # Check for sentinel value
            break
            
        def process_single_job():
            nonlocal active_threads
            try:
                processed_job = executor.execute_job(job)
                while True:
                    backoff, error = aggregator.try_add_job(processed_job)
                    if error is None:
                        break
                    if needs_backoff:
                        time.sleep(backoff * 0.0001)  # Convert to similar backoff as Go version
            finally:
                with thread_lock:
                    active_threads -= 1
                    thread_done.notify()
                    
        with thread_lock:
            active_threads += 1
            
        # Start a new thread for this job
        job_thread = Thread(target=process_single_job, daemon=True)
        job_thread.start()

    # Wait for all threads to complete
    with thread_lock:
        while active_threads > 0:
            thread_done.wait()
            
    # Signal completion to aggregator
    aggregator.try_add_job(Job(id=-100))
    print("all jobs processed")

def main():

    num_jobs = 100
    print(f"Number of jobs: {num_jobs}")
    
    # Initialize components
    producer = Producer(num_jobs)
    executor = Executor()
    aggregator = Aggregator()
    
    # Start producer in a separate thread
    producer_thread = Thread(target=producer.produce)
    producer_thread.start()
    
    # Start consumer/processor thread
    start_time = time.time()
    processor_thread = Thread(
        target=process_jobs,
        args=(producer.job_queue, aggregator, executor, True)
    )
    processor_thread.start()
    
    # Start aggregator flush thread
    flush_thread = Thread(target=aggregator.flush_results)
    flush_thread.start()

    # Wait for completion
    producer_thread.join()
    processor_thread.join()
    flush_thread.join()
    
    print(f"Average execution latency: {executor.get_avg_sleep():.2f} milliseconds")
    print(f"Total time taken: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main() 