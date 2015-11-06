"""event_type values being encountered on each day in a given time interval"""

import logging

import luigi
import luigi.task

from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin, MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url



log = logging.getLogger(__name__)


class EventTypeDistributionTask(EventLogSelectionMixin, MapReduceJobTask):


    def output(self):
        return get_target_from_url(self.output_root)

    output_root = luigi.Parameter()

    def mapper(self, line):
        print("HELLO HERE I AM !!!! \n")
        value = self.get_event_and_date_string(line)
        event, _date_string = value

        event_type = str(event.get('event_type'))
        event_date = str(event.get('time')).split("T")[0]
        if event_type.startswith('/'):
            #Ignore events that begin with a slash
            log.error("Invalid event type occurred")
            return
        yield (event_date,event_type), 1

    def reducer(self, key, values):
        event_date_type = key
        event_count = sum(values)
        print (event_date_type + " ," + event_count)
        yield (event_date_type), event_count