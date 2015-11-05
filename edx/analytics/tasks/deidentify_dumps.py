import luigi
import luigi.s3
import datetime
import csv
import json
import os

from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.id_codec import UserIdRemapperMixin
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.util.file_util import copy_file_to_open_file

import logging

log = logging.getLogger(__name__)


class BaseDeidentifyDumpTask(UserIdRemapperMixin, luigi.Task):

    course = luigi.Parameter()
    output_directory = luigi.Parameter()
    data_directory = luigi.Parameter()

    def output(self):
        if len(self.input()) == 0:
            raise IOError("Course File '{filename}' not found for course '{course}'".format(
                filename=self.file_pattern, course = self.course
            ))

        output_filename = os.path.basename(self.input()[0].path)
        return get_target_from_url(url_path_join(self.output_directory, output_filename))

    def requires(self):
        return PathSetTask([self.data_directory], [self.file_pattern])


class DeidentifySqlDumpTask(BaseDeidentifyDumpTask):

    def run(self):
        input_file = self.input()[0].open('r')
        output_file = self.output().open('w')

        writer = csv.writer(output_file, dialect='mysqlpipe')
        reader = csv.reader(input_file, dialect='mysqlpipe')

        headers = next(reader, None)

        if headers:
            writer.writerow(headers)

        for row in reader:
            # Found an empty line at the bottom of test file
            if row:
                filtered_row = self.filter_row(row)
                writer.writerow(filtered_row)

        output_file.close()

    def filter_row(self, row):
        raise NotImplementedError


class FileCopyTask(object):

    def run(self):
        def report_progress(num_bytes):
            """Update hadoop counters as the file is written"""
            self.incr_counter('FilyCopyTask', 'Bytes Written to Output', num_bytes)

        output_file = self.output().open('w')
        try:
            copy_file_to_open_file(self.input()[0].path, output_file, progress=report_progress)
        finally:
            output_file.close()


class DeidentifyAuthUserProfileTask(DeidentifySqlDumpTask):

    file_pattern = '*-auth_userprofile-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        row[2] = ''  # name
        row[3] = ''  # language
        row[4] = ''  # location
        row[5] = ''  # meta
        row[6] = ''  # courseware
        row[8] = ''  # mailing_address
        row[12] = '1'  # allow_certificate
        row[14] = ''  # city
        row[15] = ''  # bio

        return row


class DeidentifyAuthUserTask(DeidentifySqlDumpTask):

    file_pattern = '*-auth_user-*'

    def filter_row(self, row):
        row[0] = self.remap_id(row[0])  # id
        row[1] = "username_{id}".format(id=row[0])
        row[2] = ''  # first_name
        row[3] = ''  # last_name
        row[4] = ''  # email
        row[5] = ''  # passwords

        for i in xrange(11,len(row)):
            row[i] = ''

        return row


class DeidentifyStudentCourseEnrollmentTask(DeidentifySqlDumpTask):

    file_pattern = '*-student_courseenrollment-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class DeidentifyUserApiUserCourseTagTask(DeidentifySqlDumpTask):

    file_pattern = '*-user_api_usercoursetag-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class DeidentifyStudentLanguageProficiencyTask(DeidentifySqlDumpTask):

    file_pattern = '*-student_languageproficiency-*'

    def filter_row(self, row):
        return row


class DeidentifyCoursewareStudentModule(DeidentifySqlDumpTask):

    file_pattern = '*-courseware_studentmodule-*'

    def filter_row(self, row):
        row[3] = self.remap_id(row[3])  # student_id
        #row[4] = ?  # state
        return row


class DeidentifyCertificatesGeneratedCertificate(DeidentifySqlDumpTask):

    file_pattern = '*-certificates_generatedcertificate-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        row[2] = ''  # download_url
        row[8] = ''  # verify_uuid
        row[9] = ''  # download_uuid
        row[10] = ''  # name
        row[13] = ''  # error_reason
        return row


class DeidentifyTeamsTask(DeidentifySqlDumpTask):

    file_pattern = '*-teams-*'

    def filter_row(self, row):
        return row


class DeidentifyTeamsMembershipTask(DeidentifySqlDumpTask):

    file_pattern = '*-teams_membership-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class DeidentifyVerificationStatusTask(DeidentifySqlDumpTask):

    file_pattern = '*-verify_student_verificationstatus-*'

    def filter_row(self, row):
        row[4] = self.remap_id(row[4])  # user_id
        return row


class DeidentifyWikiArticleTask(DeidentifySqlDumpTask):

    file_pattern = '*-wiki_article-*'

    def filter_row(self, row):
        return row


class DeidentifyWikiArticleRevisionTask(DeidentifySqlDumpTask):

    file_pattern = '*-wiki_articlerevision-*'

    def filter_row(self, row):
        row[2] = ''  # user_message
        row[3] = ''  # automatic_log
        row[4] = ''  # ip_address
        row[5] = self.remap_id(row[5])  # user_id


class CourseStructureTask(BaseDeidentifyDumpTask, FileCopyTask):

    file_pattern = '*-course_structure-*'


class CourseContentTask(BaseDeidentifyDumpTask, FileCopyTask):

    file_pattern = '*-course-*'


class DeidentifyMongoDumpsTask(BaseDeidentifyDumpTask):

    file_pattern = '*mongo*'

    def run(self):
        if len(self.input()) == 0:
            # throw exception
            pass

        input_file = self.input()[0].open('r')
        output_file = self.output().open('w')

        for line in input_file:
            row = json.loads(line)
            filtered_row = self.filter_row(row)
            output_file.write(json.dumps(filtered_row, ensure_ascii=False).encode('utf-8'))
            output_file.write('\n')

        output_file.close()

    def filter_row(self, row):
        row['author_id'] = self.remap_id(row['author_id'])
        row['author_username'] = "username_{id}".format(id=row['author_id'])
        # scrub & encrypt ?
        return row


class DeidentifyCourseDumpTask(luigi.WrapperTask):

    course = luigi.Parameter()
    dump_root = luigi.Parameter()
    output_root = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(DeidentifyCourseDumpTask, self).__init__(*args, **kwargs)

        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        auth_userprofile_targets = PathSetTask([url_path_join(self.dump_root, filename_safe_course_id, 'state')], ['*auth_userprofile*']).output()
        auth_userprofile_paths = [target.path for target in auth_userprofile_targets]
        latest_auth_userprofile = sorted(auth_userprofile_paths)[-1]
        self.data_directory = os.path.dirname(latest_auth_userprofile)
        self.output_directory = url_path_join(self.output_root, filename_safe_course_id)

    def requires(self):
        kwargs = {
            'course': self.course,
            'output_directory': self.output_directory,
            'data_directory': self.data_directory
        }
        yield (
            DeidentifyAuthUserTask(**kwargs),
            DeidentifyAuthUserProfileTask(**kwargs),
            DeidentifyStudentCourseEnrollmentTask(**kwargs),
            DeidentifyUserApiUserCourseTagTask(**kwargs),
            DeidentifyStudentLanguageProficiencyTask(**kwargs),
            DeidentifyCoursewareStudentModule(**kwargs),
            DeidentifyCertificatesGeneratedCertificate(**kwargs),
            DeidentifyTeamsTask(**kwargs),
            DeidentifyTeamsMembershipTask(**kwargs),
            DeidentifyVerificationStatusTask(**kwargs),
            DeidentifyWikiArticleTask(**kwargs),
            DeidentifyWikiArticleRevisionTask(**kwargs),
            CourseContentTask(**kwargs),
            CourseStructureTask(**kwargs),
            DeidentifyMongoDumpsTask(**kwargs),
        )


class DeidentifyDumpsTask(luigi.WrapperTask):

    course = luigi.Parameter(is_list=True)
    dump_root = luigi.Parameter()
    output_root = luigi.Parameter(
        config_path={'section': 'rdx', 'name': 'output_root'}
    )
    output_root = luigi.Parameter(default='hdfs://localhost:9000/edx-analytics-pipeline/output/')

    def requires(self):
        for course in self.course:
            kwargs = {
                'dump_root': self.dump_root,
                'course': course,
                'output_root': self.output_root,
            }
            yield DeidentifyCourseDumpTask(**kwargs)
