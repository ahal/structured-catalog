from multiprocessing import Lock

from .base import BaseQueue

pymongo = None
def do_delayed_imports():
    global pymongo
    import pymongo

class MongoQueue(BaseQueue):
    _lock = Lock()
    dot_encoding = '!@@__@'
    max_score = None

    def __init__(self):
        do_delayed_imports()
        BaseQueue.__init__(self)
        self.client = pymongo.MongoClient('localhost', 27012)
        self.db = self.client.structured_catalog

    def encode_blobber_file_keys(job):
        blobber_files = {}
        for k, v in job['blobber_files'].iteritems():
            blobber_files[k.replace('.', self.dot_encoding)] = v
        job['blobber_files'] = blobber_files

    def decode_blobber_file_keys(job):
        blobber_files = {}
        for k, v in job['blobber_files'].iteritems():
            blobber_files[k.replace(self.dot_encoding, '.')] = v
        job['blobber_files'] = blobber_files

    def push(self, job):
        self.encode_blobber_file_keys(job)
        job = {
            'payload': job,
            'score': 0,
        }
        self.db.jobs.insert(job)

    def get(self, max_score=None):
        self._lock.acquire()
        try:
            if self.max_score is None:
                self.max_score = max_score or \
                        max(self.db.jobs.distinct('score'))

            # this assumes jobs don't need to be re-processed in order
            job = self.db.jobs.find_one({'score': {'$lte': self.max_score})
            if not job:
                return

            job['score'] = self.max_score + 1
            self.db.jobs.save(job)
            job = job['payload']
            self.decode_blobber_file_keys(job)
            return job
        finally:
            self._lock.release()

    def remove(self, job):
        job = self.db.jobs.find_one({ 'payload': job })
        self.db.jobs.remove(job)
