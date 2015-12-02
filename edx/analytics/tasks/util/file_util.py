"""
Utility methods interact with files.
"""
import logging

TRANSFER_BUFFER_SIZE = 1024 * 1024  # 1 MB
log = logging.getLogger(__name__)


class FileCopyTask(object):

    def run(self):
        def report_progress(num_bytes):
            """Update hadoop counters as the file is written"""
            self.incr_counter('FilyCopyTask', 'Bytes Written to Output', num_bytes)

        output_file = self.output().open('w')
        if isinstance(self.input(), list):
            if len(self.input()) == 1:
                filepath = self.input()[0].path
            else:
                # What should we do here, throw exception ?
        else:
            filepath = self.input().path

        try:
            copy_file_to_open_file(filepath, output_file, progress=report_progress)
        finally:
            output_file.close()


def copy_file_to_open_file(filepath, output_file, progress=None):
    """Copies a filepath to a file object already opened for writing."""
    log.info('Copying to output: %s', filepath)
    with open(filepath, 'r') as src_file:
        while True:
            transfer_buffer = src_file.read(TRANSFER_BUFFER_SIZE)
            if transfer_buffer:
                output_file.write(transfer_buffer)
                if progress:
                    try:
                        progress(len(transfer_buffer))
                    except:  # pylint: disable=bare-except
                        pass
            else:
                break
    log.info('Copy to output complete')
