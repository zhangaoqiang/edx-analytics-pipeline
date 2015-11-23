""" Test event type distribution per day in a given time interval """

import json

import luigi
from edx.analytics.tasks.answer_dist import ProblemCheckEventMixin
from edx.analytics.tasks.event_type_dist import (EventTypeDistributionTask)

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin



class EventTypeDistributionBaseTest(MapperTestMixin, ReducerTestMixin, unittest.TestCase):
    """ Base test class for Event type distribution task """

    def initialize_ids(self):
        """Define set of id values for use in tests."""
        raise NotImplementedError

    def setUp(self):
        self.task_class = ProblemCheckEventMixin
        super(EventTypeDistributionBaseTest, self).setUp()

        self.initialize_ids()
        self.username = 'test_user'
        self.user_id = 24
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.earlier_timestamp = "2013-12-15T15:38:32.805444"
        self.reduce_key = (self.event_date, self.event_type, self.event_source)  # pylint: disable=no-member

class EventTypeDistributionTaskMapTest(EventTypeDistributionBaseTest, InitializeOpaqueKeysMixin):
    """ Test if mapper works for event type distribution"""

    def test_non_problem_check_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_unparseable_problem_check_event(self):
        line = 'this is garbage but contains problem_check'
        self.assert_no_map_output_for(line)

    def test_event_type_should_not_begin_with_a_slash(self):
        line = self.create_event_log_line(event_type='/event')
        self.assert_no_map_output_for(line)

    def test_event_type_should_not_be_none(self):
        line = self.create_event_log_line(event_type=None)
        self.assert_no_map_output_for(line)




class CourseEnrollmentTaskReducerTest(EventTypeDistributionBaseTest, InitializeOpaqueKeysMixin):
    """
    Tests to verify that events-per-day-per-type reducer works correctly.
    """

    def test_no_events(self):
        self.assert_no_output([])

    def test_output(self,inputs,expected):
        """
        Args:
            inputs:array of values to pass to reducer
                for hard-coded key.
            expected: dict of expected answer data
        """
        reducer_output = self._get_reducer_output(inputs)
        for key, value in reducer_output:
            event_date, event_type, event_source = key
            count = value
            self.assertEquals(event_date, self.event_date)
            self.assertEquals(event_type, self.event_type)
            self.assertEquals(event_source, self.event_source)
            expected_key = "{}_{}".format(event_date, event_type, event_source)
            self.assertTrue(expected_key in expected)
            self.assertEquals(count, expected.get(expected_key))