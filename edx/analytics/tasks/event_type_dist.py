"""event_type values being encountered on each day in a given time interval"""

import logging
import luigi
import luigi.task
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin, MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class EventTypeDistributionTask(EventLogSelectionMixin, MapReduceJobTask):
    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'testfile.txt'))

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        event, _date_string = value
        event_type = str(event.get('event_type'))
        event_date = str(event.get('time')).split("T")[0]
        if event_type.startswith('/'):
            # Ignore events that begin with a slash
            return
        yield (event_date, event_type), 1

    def reducer(self, key, values):
        event_date_type = key
        event_count = sum(values)
        yield (event_date_type), event_count
