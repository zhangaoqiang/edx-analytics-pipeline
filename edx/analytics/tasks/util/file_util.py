"""
Utility methods interact with files.
"""
import logging

TRANSFER_BUFFER_SIZE = 1024 * 1024  # 1 MB
log = logging.getLogger(__name__)


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
