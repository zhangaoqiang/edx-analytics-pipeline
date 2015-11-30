"""event_type and event_source values being encountered on each day in a given time interval"""

import logging
import luigi
import luigi.task
from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import ExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.vertica_load import VerticaCopyTask

log = logging.getLogger(__name__)


class EventTypeDistributionTask(EventLogSelectionMixin, MapReduceJobTask):
    """task to compute event_type and event_source values being encountered on each day in a given time interval"""
    output_root = luigi.Parameter()

    events_list_file_path = "hdfs://localhost:9000/data/event_list.tsv"
    known_events = {}

    def requires_local(self):
        return ExternalURL(url=self.events_list_file_path)

    def init_local(self):
        super(EventTypeDistributionTask, self).init_local()
        #self.input_local().open() as f_in
        with self.input_local().open() as f_in:
            lines = filter(None, (line.rstrip() for line in f_in))

        for line in lines:
            if(not line.startswith('#')):
                parts = line.split(" ")
                self.known_events[(parts[1],parts[2])] = parts[0]
        log.debug("==================== Umer Debugging Logs =================")
        log.debug(self.known_events)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value
        event_type = event.get('event_type')
        event_date = date_string
        event_source = event.get('event_source')
        if event_source is None or event_type is None or event_date is None:
            # Ignore if any of the keys is None
            return
        if event_type.startswith('/'):
            # Ignore events that begin with a slash
            return
        if (event_source,event_type) in self.known_events:
            event_category = self.known_events[(event_source,event_type)]
        else:
            event_category = 'unknown'

        yield (event_date, event_category, event_type, event_source), 1

    def reducer(self, key, values):
        event_date_type_source = key
        event_count = sum(values)
        yield (event_date_type_source), event_count

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'event_type_distribution/'))


class PushToVerticaEventTypeDistributionTask(VerticaCopyTask):
    """push the event type distribution task data to vertica"""
    output_root = luigi.Parameter()
    interval = luigi.DateIntervalParameter()
    n_reduce_tasks = luigi.Parameter()


    @property
    def table(self):
        return "event_type_distribution"

    @property
    def columns(self):
        return [
            ('event_date', 'DATETIME'),
            ('event_category', 'VARCHAR(255)'),
            ('event_type', 'VARCHAR(255)'),
            ('event_source', 'VARCHAR(255)'),
            ('event_count', 'INT'),
        ]

    @property
    def insert_source_task(self):
        return EventTypeDistributionTask(
            output_root=self.output_root,
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
        )