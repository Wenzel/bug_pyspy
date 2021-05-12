#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
import time
from random import random
import threading
from uuid import uuid4
from threading import Condition


TASK_NB = 10


class Pipeline:

    def __init__(self):
        self._pool = ThreadPoolExecutor(max_workers=4)
        # condition variable
        self._cv = Condition()
        self._counter = 0

    def __call__(self):
        for i in range(0, TASK_NB):
            task_id = str(uuid4())
            print(f'[task: {task_id}] pipeline start')
            future = self._pool.submit(self.stage1, task_id)
            future.add_done_callback(self.pipe_1_to_2)
        with self._cv:
            # wait until all tasks went through the pipeline
            self._cv.wait_for(lambda: self._counter == TASK_NB)


    def slow_func(self, task_id: str, factor: float):
        rvalue: float = random() * factor
        tid: int= threading.get_ident()
        print(f"[task: {task_id}] processing for {rvalue:.{2}} seconds... (factor: {factor})")
        time.sleep(rvalue)
        return task_id


    def stage1(self, task_id: str):
        return self.slow_func(task_id, 2)


    def pipe_1_to_2(self, f):
        task_id = f.result()
        future = self._pool.submit(self.stage2, task_id)
        future.add_done_callback(self.pipeline_end)


    def stage2(self, task_id: str):
        return self.slow_func(task_id, 1.5)

    def pipeline_end(self, f):
        task_id = f.result()
        print(f'[task: {task_id}] pipeline done')
        with self._cv:
            # increment counter
            self._counter += 1
            # notify waiters
            self._cv.notify_all()


def main():
    pipe = Pipeline()
    pipe()


if __name__ == '__main__':
    main()
