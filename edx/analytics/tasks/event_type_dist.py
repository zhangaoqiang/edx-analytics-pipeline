"""event_type values being encountered on each day in a given time interval"""

import logging

import luigi
import luigi.task

from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin, MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin


log = logging.getLogger(__name__)


class EventTypeDistributionTask(EventLogSelectionMixin, MapReduceJobTask):
    """Produce a dataset which tells which type of events occured in a given time interval"""
    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        event, _date_string = value

        event_type = str(event.get('event_type'))
        if event_type.startswith('/'):
            #Ignore events that begin with a slash
            log.error("Invalid event type occurred")
            return
        yield (event_type), 1
        pass

    def reducer(self, key, values):
        event_type = key
        event_count = sum(values)
        yield (event_type), event_count
        pass
