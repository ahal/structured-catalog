from multiprocessing import Lock

from .base import BaseQueue

pymongo = None
def do_delayed_imports():
    global pymongo
    import pymongo

class MongoQueue(BaseQueue):
    _lock = Lock()
    processed_count = None

    def __init__(self):
        do_delayed_imports()
        BaseQueue.__init__(self)
        self.client = pymongo.MongoClient('localhost', 27012)
        self.db = self.client.structured_catalog

    def push(self, job):
        job = {
            'payload': job,
            'processed_count': 0,
        }
        self.db.jobs.insert(job)

    def get(self, processed_count=None):
        self._lock.acquire()
        try:
            if self.processed_count is None:
                self.processed_count = processed_count or \
                        min(self.db.jobs.distinct('processed_count'))

            # this assumes jobs don't need to be re-processed in order
            job = self.db.jobs.find_one({'processed_count': self.processed_count})
            if not job:
                return

            job['processed_count'] += 1
            self.db.jobs.save(job)
            return job['payload']
        finally:
            self._lock.release()

    def remove(self, job):
        job = self.db.jobs.find_one({ 'payload': job })
        self.db.jobs.remove(job)
