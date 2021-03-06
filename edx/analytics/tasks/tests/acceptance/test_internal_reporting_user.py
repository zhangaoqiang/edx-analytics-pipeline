"""
End to end test of the internal reporting user table loading task.
"""

import os
import logging
import datetime

import pandas

from luigi.date_interval import Date

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join


log = logging.getLogger(__name__)


class InternalReportingUserLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's user table."""

    INPUT_FILE = 'location_by_course_tracking.log'
    DATE_INTERVAL = Date(2014, 7, 21)

    def setUp(self):
        super(InternalReportingUserLoadAcceptanceTest, self).setUp()

        # Set up the mock LMS databases.
        self.execute_sql_fixture_file('load_auth_user_for_internal_reporting_user.sql')
        self.execute_sql_fixture_file('load_auth_userprofile.sql')

        # Put up the mock tracking log for user locations.
        self.upload_tracking_log(self.INPUT_FILE, datetime.datetime(2014, 7, 21))

    def test_internal_reporting_user(self):
        """Tests the workflow for the internal reporting user table, end to end."""

        self.task.launch([
            'LoadInternalReportingUserToWarehouse',
            '--interval', self.DATE_INTERVAL.to_string(),
            '--user-country-output', url_path_join(self.test_out, 'user'),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_user.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=True)

            cursor.execute("SELECT * FROM {schema}.d_user".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            d_user = pandas.DataFrame(response, columns=['user_id', 'user_year_of_birth', 'user_level_of_education',
                                                         'user_gender', 'user_email', 'user_username',
                                                         'user_account_creation_time',
                                                         'user_last_location_country_code'])

            try:  # A ValueError will be thrown if the column names don't match or the two data frames are not square.
                self.assertTrue(all(d_user == expected))
            except ValueError:
                self.fail("Expected and returned data frames have different shapes or labels.")
