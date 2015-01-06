from multiprocessing import Lock

from .base import BaseQueue

pymongo = None
def do_delayed_imports():
    global pymongo
    import pymongo

class MongoQueue(BaseQueue):
    _lock = Lock()
    processed_count = None
    dot_encoding = '\x95\x25\xab\x6e'

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
            job = job['payload']
            self.decode_blobber_file_keys(job)
            return job
        finally:
            self._lock.release()

    def remove(self, job):
        job = self.db.jobs.find_one({ 'payload': job })
        self.db.jobs.remove(job)
