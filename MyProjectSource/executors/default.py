#   Copyright (C) 2022  Carl Chatterton. All Rights Reserved.
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.
from maellin.executors.base import BaseExecutor
from maellin.utils import get_task_result
from maellin.logger import LoggingMixin
from typing import TypeVar


Queue = TypeVar('Queue')


class DefaultWorker(LoggingMixin):
    """
    A Local based Worker that processes tasks sequentially.
    This worker does not support concurrency features
    """
    worker_id = 0

    def __init__(self, task_queue: Queue, result_queue: Queue):
        DefaultWorker.worker_id += 1
        self.task_queue = task_queue
        self.result_queue = result_queue
        self._log = self.logger

    def run(self):

        while not self.task_queue.empty():
            # Get the activity from the queue to process
            _task = self.task_queue.get()
            _task.update_status('Running')
            self._log.info('Running Task %s on Worker %s ' % (_task.name, self.worker_id))

            # Get inputs to use from dependencies
            if _task.depends_on:
                inputs = ()
                for dep_task in list(dict.fromkeys(_task.depends_on).keys()):
                    for completed_task in list(self.result_queue.queue):
                        if dep_task.tid == completed_task.tid:
                            input_data = get_task_result(completed_task)
                            inputs = inputs + input_data
            else:
                inputs = tuple()

            # Run the task with instructions
            _task.run(inputs)
            _task.update_status('Completed')

            # Put the results of the complete task in the result queue
            self.result_queue.put(_task)

            # Activity is finished running
            self.task_queue.task_done()


class DefaultExecutor(BaseExecutor):
    """Executes Tasks Sequentially using a single worker"""

    def __init__(self, task_queue, result_queue):
        super().__init__(task_queue, result_queue)

    def start(self):
        self._log.info('Starting Job %s' % self.job_id)
        self.worker = DefaultWorker(self.task_queue, self.result_queue)
        return self.worker.run()

    def end(self):
        """Removes the worker and results"""
        del self.worker