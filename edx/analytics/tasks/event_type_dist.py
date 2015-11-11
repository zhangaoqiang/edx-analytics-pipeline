"""event_type values being encountered on each day in a given time interval"""

import logging
import luigi
import luigi.task
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin, MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.vertica_load import VerticaCopyTask

log = logging.getLogger(__name__)


class EventTypeDistributionTask(EventLogSelectionMixin, MapReduceJobTask, VerticaCopyTask):

    output_root = luigi.Parameter()

    src = luigi.Parameter(is_list=True)

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

    def output(self):
            return get_target_from_url(url_path_join(self.output_root, 'testfile.txt'))

    @property
    def table(self):
        return "event_type_distribution"

    @property
    def columns(self):
        return [
            ('date', 'DATETIME'),
            ('event_type', 'VARCHAR(255)'),
            ('event_count', 'INT'),
        ]

    @property
    def insert_source_task(EventTypeDistributionTask):
        return None
