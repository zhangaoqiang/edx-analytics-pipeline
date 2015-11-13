""" Test event type distribution per day in a given time interval """

import json

import luigi

from edx.analytics.tasks.event_type_dist import (EventTypeDistributionTask)

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin


class EventTypeDistributionTaskMapTest(MapperTestMixin, InitializeOpaqueKeysMixin, unittest.TestCase):

    def setUp(self):
        self.task_class = EventTypeDistributionTask
        super(EventTypeDistributionTask, self).setUp()

        self.interval = '2014-01-01-2015-12-30'
        self.src = 'hdfs://localhost:9000/data/tracking.log'
        self.output_root = 'hdfs://localhost:9000/edx-analytics-pipeline/output'
        self.n_reduce_tasks = 1

        self.event_templates = {
            'event': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "edx.course.enrollment.activated",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.user_id,
                },
                "time": "{0}+00:00".format(self.timestamp),
                "ip": "127.0.0.1",
                "event": {
                    "course_id": self.course_id,
                    "user_id": self.user_id,
                    "mode": "honor",

                }
            }
        }

        self.default_event_template = 'event'

        self.expected_key = (self.event_type, self.event_date)


    def test_event_is_none(self):
        line = '{"username": "staff", "event_source": "browser", "name": "problem_check", "accept_language": "en-US,en;q=0.8", "time": "2015-08-17T20:51:13.628738+00:00", "agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.125 Safari/537.36", "page": "http://localhost:8000/courses/course-v1:edX+DemoX+Demo_Course/courseware/interactive_demonstrations/basic_questions/", "host": "precise64", "session": "618be66ec3fbf8e9d80f664704a7c706", "referer": "http://localhost:8000/courses/course-v1:edX+DemoX+Demo_Course/courseware/interactive_demonstrations/basic_questions/", "context": {"user_id": 4, "org_id": "edX", "course_id": "course-v1:edX+DemoX+Demo_Course", "path": "/event"}, "ip": "10.0.2.2", "event": "input_a0effb954cca4759994f1ac9e9434bf4_2_1=blue&input_a0effb954cca4759994f1ac9e9434bf4_3_1=choice_0&input_a0effb954cca4759994f1ac9e9434bf4_4_1%5B%5D=choice_0&input_a0effb954cca4759994f1ac9e9434bf4_4_1%5B%5D=choice_2", "event_type": ""}'
        self.assert_no_map_output_for(line)

    def test_event_begins_with_a_slash(self):
        line = '{"username": "staff", "event_source": "browser", "name": "problem_check", "accept_language": "en-US,en;q=0.8", "time": "2015-08-17T20:51:13.628738+00:00", "agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.125 Safari/537.36", "page": "http://localhost:8000/courses/course-v1:edX+DemoX+Demo_Course/courseware/interactive_demonstrations/basic_questions/", "host": "precise64", "session": "618be66ec3fbf8e9d80f664704a7c706", "referer": "http://localhost:8000/courses/course-v1:edX+DemoX+Demo_Course/courseware/interactive_demonstrations/basic_questions/", "context": {"user_id": 4, "org_id": "edX", "course_id": "course-v1:edX+DemoX+Demo_Course", "path": "/event"}, "ip": "10.0.2.2", "event": "input_a0effb954cca4759994f1ac9e9434bf4_2_1=blue&input_a0effb954cca4759994f1ac9e9434bf4_3_1=choice_0&input_a0effb954cca4759994f1ac9e9434bf4_4_1%5B%5D=choice_0&input_a0effb954cca4759994f1ac9e9434bf4_4_1%5B%5D=choice_2", "event_type": "/event"}'
        self.assert_no_map_output_for(line)


class CourseEnrollmentTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """
    Tests to verify that events-per-day-per-type reducer works correctly.
    """
    def setUp(self):
        self.task_class = EventTypeDistributionTask
        super(CourseEnrollmentTaskReducerTest, self).setUp()
        # Create the task locally, since we only need to check certain attributes
        self.create_task()
        self.event_type = 'problem_check'
        self.event_date = '2015-08-17'
        self.reduce_key = (self.event_date, self.event_type)

    def test_no_events(self):
        self.assert_no_output([])

    def test_single_event(self):
        inputs = [(('2013-01-01T00:00:01', "problem_check"), 1), ]
        expected = (('2013-01-01', "problem_check", 1),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_events(self):
        inputs = [
            (('2013-01-01T00:00:01', "problem_check"), 1),
            (('2013-01-01T00:00:01', "problem_check"), 1),
            (('2013-02-01T00:00:01', "problem_check"), 1),
            (('2013-02-01T00:00:01', "dummy_event"), 1),
            (('2013-03-01T00:00:01', "dummy_event_2"), 1),
        ]
        expected = (('2013-01-01T00:00:01', "problem_check", 2),
                    ('2013-02-01T00:00:01', "problem_check", 1),
                    ('2013-02-01T00:00:01', "dummy_event", 1),
                    ('2013-03-01T00:00:01', "dummy_event_2", 1),)
        self._check_output_complete_tuple(inputs, expected)