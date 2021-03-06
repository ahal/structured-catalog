from datetime import datetime
from multiprocessing import Lock, Value

from .base import BaseQueue

pymongo = None
def do_delayed_imports():
    global pymongo
    import pymongo

class MongoQueue(BaseQueue):
    _lock = Lock()
    max_score = Value('i', -1)
    dot_encoding = '!@@__@'

    def __init__(self):
        do_delayed_imports()
        BaseQueue.__init__(self)
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client.structured_catalog
        # structured logs are only kept for a month, automatically expire documents
        self.db.jobs.ensure_index('created', expireAfterSeconds=60*60*24*31)

    def encode_blobber_file_keys(self, job):
        blobber_files = {}
        for k, v in job['blobber_files'].iteritems():
            blobber_files[k.replace('.', self.dot_encoding)] = v
        job['blobber_files'] = blobber_files

    def decode_blobber_file_keys(self, job):
        blobber_files = {}
        for k, v in job['blobber_files'].iteritems():
            blobber_files[k.replace(self.dot_encoding, '.')] = v
        job['blobber_files'] = blobber_files

    def push(self, job):
        self.encode_blobber_file_keys(job)
        job = {
            'payload': job,
            'score': 0,
            'created': datetime.utcnow(),
        }
        self.db.jobs.insert(job)

    def get(self):
        self._lock.acquire()
        try:
            if self.max_score.value == -1:
                self.max_score.value = max(self.db.jobs.distinct('score'))

            # this assumes jobs don't need to be re-processed in order
            job = self.db.jobs.find_one({'score': {'$lte': self.max_score.value}})
            if not job:
                return

            job['score'] = self.max_score.value + 1
            self.db.jobs.save(job)
            job = job['payload']
            self.decode_blobber_file_keys(job)
            return job
        finally:
            self._lock.release()

    def remove(self, job):
        job = self.db.jobs.find_one({ 'payload': job })
        self.db.jobs.remove(job)
