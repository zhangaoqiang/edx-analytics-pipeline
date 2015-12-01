"""
End to end test of the financial reporting workflow.
"""

import logging
import os
from cStringIO import StringIO
import datetime


from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

log = logging.getLogger(__name__)


class EngagementAcceptanceTest(AcceptanceTestCase):

    INPUT_FILE = 'engagement_acceptance_tracking_{date}.log'
    NUM_REDUCERS = 1

    def test_engagement(self):
        for day in (13, 16):
            dt = datetime.date(2015, 4, day)
            self.upload_tracking_log(self.INPUT_FILE.format(date=dt.strftime('%Y%m%d')), dt)

        # First run with the 13th
        self.task.launch([
            'EngagementIntervalTask',
            '--interval', '2015-04-13',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        # Verify the output
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT date, course_id, username, entity_type, entity_id, event, count FROM engagement'
                ' ORDER BY date, course_id, username, entity_type, entity_id, event ASC'
            )
            results = cursor.fetchall()

        april_thirteenth = datetime.date(2015, 4, 13)
        expected_first_day = [
            (april_thirteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'commented', 1),
            (april_thirteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'created', 1),
            (april_thirteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'responded', 1),
            (april_thirteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'video', '8c0028eb2a724f48a074bc184cd8635f', 'played', 1),
            (april_thirteenth, 'edX/DemoX/Demo_Course', 'honor', 'video', 'i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 'played', 2),
            (april_thirteenth, 'edX/DemoX/Demo_Course_2', 'honor', 'video', 'i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 'played', 1),
            (april_thirteenth, 'edX/DemoX/Demo_Course_2', 'staff', 'video', 'i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 'played', 1),
        ]
        self.assertListEqual(expected_first_day, results)

        # Then run the 16th
        self.task.launch([
            'EngagementIntervalTask',
            '--interval', '2015-04-16',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        # Verify the output, this should include both the first and second day!
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT date, course_id, username, entity_type, entity_id, event, count FROM engagement'
                ' ORDER BY date, course_id, username, entity_type, entity_id, event ASC'
            )
            results = cursor.fetchall()

        april_sixteenth = datetime.date(2015, 4, 16)
        expected_second_day = [
            (april_sixteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'commented', 1),
            (april_sixteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'created', 1),
            (april_sixteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'responded', 1),
            (april_sixteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'video', '8c0028eb2a724f48a074bc184cd8635f', 'played', 1),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'commented', 3),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'created', 1),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'responded', 2),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02', 'attempted', 1),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/75f9562c77bc4858b61f907bb810d974', 'attempted', 2),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/75f9562c77bc4858b61f907bb810d974', 'completed', 1),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/a0effb954cca4759994f1ac9e9434bf4', 'attempted', 3),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/a0effb954cca4759994f1ac9e9434bf4', 'completed', 1),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'video', 'i4x-edX-DemoX-video-0b9e39477cf34507a7a48f74be381fdd', 'played', 2),
            (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'video', 'i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 'played', 1),
            (april_sixteenth, 'edX/DemoX/Demo_Course_2', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'commented', 1),
            (april_sixteenth, 'edX/DemoX/Demo_Course_2', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'created', 1),
            (april_sixteenth, 'edX/DemoX/Demo_Course_2', 'honor', 'forum', 'cba3e4cd91d0466b9ac50926e495b76f', 'responded', 1),
            (april_sixteenth, 'edX/DemoX/Demo_Course_2', 'honor', 'problem', 'i4x://edX/DemoX/problem/a0effb954cca4759994f1ac9e9434bf4', 'attempted', 1),
        ]
        self.assertListEqual(expected_first_day + expected_second_day, results)
