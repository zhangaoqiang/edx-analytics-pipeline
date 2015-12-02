""" test event type distribution task """
import textwrap

import luigi
import os
from ddt import ddt, data, unpack
from edx.analytics.tasks.event_type_dist import EventTypeDistributionTask
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin


class EventTypeDistributionTaskMapTest(MapperTestMixin, InitializeOpaqueKeysMixin, unittest.TestCase):
    """Tests to check if event type distribution task mapper works"""

    def setUp(self):
        self.task_class = EventTypeDistributionTask
        super(EventTypeDistributionTaskMapTest, self).setUp()

        self.event_date = '2013-12-17'
        self.event_type = "test_event"
        self.event_source = "browser"
        self.event_category = "unknown"
        self.exported = False
        self.event_templates = {
            'event': {
                "username": "test_user",
                "host": "test_host",
                "event_source": self.event_source,
                "event_type": self.event_type,
                "context": {
                    "course_id": "course_id",
                    "org_id": "org_id",
                    "user_id": "user_id",
                },
                "time": self.event_date,
                "ip": "127.0.0.1",
                "event": {
                    "course_id": "course_id",
                    "user_id": "user_id",
                    "mode": "honor",
                }
            }
        }
        self.default_event_template = 'event'
        self.expected_key = (self.event_date, self.event_category, self.event_type, self.event_source, self.exported)

    def test_no_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_event_type_none(self):
        line = self.create_event_log_line(event_type=None)
        self.assert_no_map_output_for(line)

    def test_event_date_none(self):
        line = self.create_event_log_line(time=None)
        self.assert_no_map_output_for(line)

    def test_event_source_none(self):
        line = self.create_event_log_line(event_source=None)
        self.assert_no_map_output_for(line)

    def test_event_type_contains_slash(self):
        line = self.create_event_log_line(event_type="/event")
        self.assert_no_map_output_for(line)

    def test_bad_event_date(self):
        line = self.create_event_log_line(time='this is a bogus time')
        self.assert_no_map_output_for(line)

    def test_valid_event_type(self):
        line = self.create_event_log_line()
        expected_value = 1
        self.assert_single_map_output(line, self.expected_key, expected_value)

    def event_list_parsing(self):
        """ Test if the file file parsing works correct."""
        self.events_list_file_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'events_list.tsv')
        parsing_result = self.event_list_parsing()
        expected_parsing = {("browser", "edx.instructor.report.downloaded"): "admin",
                            ("server", "add-forum-admin"): "admin",
                            ("server", "add-forum-community-TA"): "admin",
                            ("server", "add-forum-mod"): "admin",
                            ("server", "add-instructor"): "admin"}
        self.assertEquals(parsing_result, expected_parsing)

    def reformat(self, string):
        """Reformat string to make it like a TSV."""
        return textwrap.dedent(string).strip().replace(' ', '\t')


@ddt
class EventTypeDistributionTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """Tests to check if event type distribution reducer works"""

    def setUp(self):
        self.task_class = EventTypeDistributionTask
        super(EventTypeDistributionTaskReducerTest, self).setUp()

        # Create the task locally, since we only need to check certain attributes
        self.create_task()
        self.event_type = "test_event"
        self.event_date = "2013-01-01"
        self.event_source = "browser"

    @data(
        (('2013-01-01', "test_event", "browser", "unknown", False), [1])
    )
    @unpack
    def test_single_event(self, reduce_key, values):
        self.reduce_key = reduce_key
        expected = ((reduce_key, 1),)
        self._check_output_complete_tuple(values, expected)

    @data(
        (('2013-01-01', "test_event", "browser", "admin", True), [1, 1, 1, 1]),
        (('2013-01-01', "test_event", "server", "test_category", True), [1, 1, 1]),
        (('2013-01-02', "test_event", "browser", "unknown", False), [1, 1, 1, 1, 1]),
        (('2013-01-03', "test_event", "server", "test_category", True), [1, 1]),
        (('2013-01-04', "test_event", "mobile", "admin", True), [1, 1, 1, 1]),
    )
    @unpack
    def test_multiple_events(self, reduce_key, values):
        self.reduce_key = reduce_key
        expected = ((reduce_key, sum(values)),)
        self._check_output_complete_tuple(values, expected)

    def create_task(self, interval='2013-01-01'):
        """Create a task for testing purposes."""
        fake_param = luigi.DateIntervalParameter()
        self.task = EventTypeDistributionTask(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
            n_reduce_tasks=1,
        )
