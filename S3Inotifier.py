'''

Monitors a directory

@author: Alastair McCormack
@license: MIT License

'''

import pyinotify  # @UnresolvedImport
import Queue
import logging
import os.path
from collections import namedtuple
from threading import Thread
from datetime import datetime
import hds_seg_fragmenter
from _collections import deque
import glob
import sys
import time
from Queue import Empty
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from hds_seg_fragmenter import HDSSegSplitterException

FILE_PROCESSOR_THREAD_COUNT = 20
S3_UPLOADER_THREAD_COUNT = 20
THREAD_TIMEOUT = 10

PROCESSED_FRAGMENT_INDEX_LENGTH = 2000

TransferFile = namedtuple("TransferFile", ["create_time", "remote_filename", "payload", "content_type"])

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

log = logging.getLogger("S3Inotifer")
log.addHandler(NullHandler())
log.setLevel(logging.DEBUG)

class S3UploadAdapter(object):
    """ Reference upload class. Could easily implement FTP or SCP """
    
    def __init__(self, bucket_name, access_key, secret):
        self.s3_conn = S3Connection(access_key, secret)
        self.bucket = self.s3_conn.get_bucket(bucket_name)

    def upload(self, filename, contents_bytes, content_type=None):
        file_key = Key(bucket=self.bucket, name=filename)
        
#         if file_key.exists():
#             log.info("File already exists on S3: %s", filename)
#             return
                
        if content_type:
            file_key.content_type = content_type
        
        log.info("Setting content_type of %s as %s", filename, content_type)
        file_key.content_type = content_type
        
        log.info("Uploading %s", filename)
        file_key.set_contents_from_string(contents_bytes, replace=True)
        return True

class UploadQueueProcessor(Thread):
    
    def __init__(self, base_directory, file_send_queue, file_adapter):
        Thread.__init__(self)
        self.go = True
        
        self.base_directory = base_directory
        self.file_queue = file_send_queue
        self.file_adapter = file_adapter
        
    def stop(self):
        self.go = False
        
    def run(self):
        while self.go:
            tf = None
            
            # Allow this thread to end
            try:
                tf = self.file_queue.get(block=True, timeout=THREAD_TIMEOUT)
            except Empty:
                continue
            
            if tf:
                filename = os.path.join(self.base_directory, "hds", tf.remote_filename)
                
                if self.file_adapter.upload(filename=filename,
                             contents_bytes=tf.payload, content_type=tf.content_type):
                    time_to_upload = datetime.now() - tf.create_time 
                    log.info("Uploading of %s took %d seconds from modification", tf.remote_filename, time_to_upload.seconds)

class EventHandler(pyinotify.ProcessEvent):
    """ Picks up inotify events and moves them to file_processor_queue """
    
    def my_init(self, file_queue):
        self.file_queue = file_queue
    
    def process_IN_CLOSE_WRITE(self, event):
        log.debug("IN_CLOSE_WRITE: %s", event.pathname)
        self.file_queue.put_nowait(event.pathname)
     
    def process_IN_MOVED_TO(self, event):
        log.debug("IN_MOVED_TO: %s", event.pathname)
        self.file_queue.put_nowait(event.pathname) 

    def process_IN_MODIFY(self, event):
        log.debug("IN_IN_MODIFY: %s", event.pathname)
        self.file_queue.put_nowait(event.pathname) 
        
class FileProcessor(Thread):
    """ Picks up events from file_processor_queue and adds files and fragments
    to data to file_send_queue """
    
    def __init__(self, file_processor_queue, file_send_queue, processed_frags):
        Thread.__init__(self)
        self.file_processor_queue = file_processor_queue
        self.file_send_queue = file_send_queue
    
        self.go = True
        self.processed_frags = processed_frags
        
    def stop(self):
        self.go = False
        
    def run(self):
        while self.go:
            event = None
            
            # Allow this thread to end
            try:
                event = self.file_processor_queue.get(block=True, timeout=THREAD_TIMEOUT)
            except Empty:
                continue
            
            if event:
                log.debug("Processing %s", event)
                
                extension = os.path.splitext(event)[1].lower()
                
                if extension == ".f4x":
                    # Split .f4x files into fragments
                    
                    f4x_filename = event
                    
                    try:
                        splitter = hds_seg_fragmenter.HDSSegSplitter(f4x_filename)
                        for fragment in splitter.split():
                            # currently refragments previously fragmented fragments
                            remote_filename = "{stream_name}Seg{segment_number}-Frag{fragment_number}".format(stream_name=splitter.stream_name,
                                                                                                            segment_number=fragment.segment_number,
                                                                                                            fragment_number=fragment.number)
                            # skip if seen before
                            if remote_filename in self.processed_frags:
                                log.debug("Skipping previously processed fragment: %s", remote_filename)
                                continue
                            
                            payload = fragment.data
                            tf = TransferFile(create_time=datetime.now(),
                                          remote_filename=remote_filename,
                                          payload=payload,
                                          content_type="video/f4f")
                        
                            log.debug("Adding %s to send queue", remote_filename)
                            self.file_send_queue.put(tf)
                            log.debug("Adding %s to processed_frags", remote_filename)
                            self.processed_frags.append(remote_filename)
                            
                    except HDSSegSplitterException as e:
                        log.warn("Problem while processing %s: %s", event, e)
                    
                elif extension in [".bootstrap", ".f4m"]:
                    payload = open(event, "rb").read()
                    remote_filename = os.path.basename(event)
                    
                    mime_types = {".bootstrap": "application/binary",
                                  ".f4m":       "application/f4m"}
                    
                    tf = TransferFile(create_time=datetime.now(),
                                      remote_filename=remote_filename,
                                      payload=payload,
                                      content_type=mime_types[extension])
                    
                    log.debug("Sleeping a bit")
                    time.sleep(3)
                    log.debug("Adding %s to send queue", remote_filename)
                    self.file_send_queue.put(tf)
                    
                else:
                    log.debug("No action defined for: %s", event)
                    

class S3HDSAutoUploader(object):
    
    def main(self):
        self._parse_args()
        self._setup_logging()
        
        self.file_send_queue = Queue.Queue()
        self.file_processor_queue = Queue.Queue()
    
        self.threads = []
        
        self._start_threads()
        #self.add_existing_files_to_queue()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
            
    def _parse_args(self):
        import argparse
    
        parser = argparse.ArgumentParser(description='Automatically turn f4f into HDS fragments and send them to S3')
        
        parser.add_argument('-s', "--source", dest="source_dir",
                            required=True,
                            default=".",
                            help="Source monitoring directory (default: %(default)s)")
        
        parser.add_argument('-d', "--destination", dest="destination_dir",
                            default=".",
                            help="Destination directory (default: %(default)s)")
        
        parser.add_argument('-D', "--debug", dest="debug", action="store_true",
                            default=False,
                            help="Enable debug")
        
        parser.add_argument('-Q', "--quiet", dest="quiet", action="store_true",
                            default=False,
                            help="Quite mode (WARNING)")
        
        parser.add_argument('-b', "--bucket", dest="destination bucket",
                            required=True,
                            help="AWS bucket name")
        
        parser.add_argument('-a', "--access-key", dest="access_key",
                            default=None, required=False,
                            help="AWS Access Key. (default: Uses boto initialisation: http://boto.readthedocs.org/en/latest/boto_config_tut.html")
            
        parser.add_argument('-s', "--secret", dest="secret",
                            default=None, required=False,
                            help="AWS Secret. (default: Uses boto initialisation: http://boto.readthedocs.org/en/latest/boto_config_tut.html")

        self.args = parser.parse_args()
            
    def _setup_logging(self):
        if self.args.debug:
            log_level = logging.DEBUG
        elif self.args.quiet:
            log_level = logging.WARNING
        else:
            log_level = logging.INFO

        logging.basicConfig(level=logging.WARN,format="%(threadName)s:%(levelname)s:%(name)s:%(message)s")
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.setLevel(log_level)
   
    def _start_threads(self):
        processed_frags = deque(maxlen=PROCESSED_FRAGMENT_INDEX_LENGTH)
        # File / fragment processor
        for _ in xrange(FILE_PROCESSOR_THREAD_COUNT):
            file_processor = FileProcessor(self.file_processor_queue,
                                           self.file_send_queue,
                                           processed_frags=processed_frags)
            self.log.info("Starting File Processor Thread")
            file_processor.start()
            self.threads.append(file_processor)
            
        # S3 Uploader
        for _ in xrange(S3_UPLOADER_THREAD_COUNT):
            s3_adapter = S3UploadAdapter(bucket_name=self.args.bucket, 
                                         access_key=self.args.access_key,
                                         secret=self.args.secret)
            
            s3_uploader = UploadQueueProcessor(base_directory="/",
                                               file_send_queue=self.file_send_queue,
                                               file_adapter=s3_adapter) 
                                                 
            self.log.info("Starting S3 Uploader Thread")
            s3_uploader.start()
            self.threads.append(s3_uploader)
            
            
        wm = pyinotify.WatchManager()  # Watch Manager
        mask = pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO | pyinotify.IN_MODIFY # watched events
        notifier = pyinotify.ThreadedNotifier(wm, EventHandler(file_queue=self.file_processor_queue))
        wm.add_watch(self.args.source_dir, mask, rec=False)
        self.log.info("Starting inotify thread")
        notifier.start()
        self.threads.append(notifier)
        
    def stop(self):
        for mthread in self.threads:
            self.log.debug("Stopping %s", mthread)
            mthread.stop()
        sys.exit(1)

                            
    def add_existing_files_to_queue(self):
        self.log.debug("Adding existing files in: %s", self.args.source_dir)
        existing_files = []
        file_exts = ["*.f4x", "*.f4m", "*.bootstrap"]
        
        for file_ext in file_exts:
            path = os.path.join(self.args.source_dir, file_ext)
            existing_file_list = glob.glob(path)
            existing_files.extend(existing_file_list)
        
        for filename in existing_files:
            self.log.debug("Adding existing file %s to process queue", filename)
            self.file_processor_queue.put_nowait(filename)
            


if __name__ == "__main__":
    S3HDSAutoUploader().main()