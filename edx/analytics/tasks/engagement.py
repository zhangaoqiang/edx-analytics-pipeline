import datetime
import logging

log = logging.getLogger(__name__)

import luigi
import luigi.task
from luigi import date_interval
from luigi.configuration import get_config

try:
    import mysql.connector
    from mysql.connector.errors import ProgrammingError
    from mysql.connector import errorcode
    mysql_client_available = True
except ImportError:
    log.warn('Unable to import mysql client libraries')
    # On hadoop slave nodes we don't have mysql client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    mysql_client_available = False

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.mysql_load import MysqlInsertTask

from edx.analytics.tasks.util.hive import (
    WarehouseMixin, BareHiveTableTask, HivePartitionTask
)
from edx.analytics.tasks.util.record import Record, StringField, IntegerField, DateField


class EngagementRecord(Record):
    """Represents a count of interactions performed by a user on a particular entity (usually a module in a course)"""

    course_id = StringField(length=255, nullable=False)
    username = StringField(length=30, nullable=False)
    date = DateField(nullable=False)
    entity_type = StringField(length=10, nullable=False)
    entity_id = StringField(length=255, nullable=False)
    event = StringField(length=30, nullable=False)
    count = IntegerField(nullable=False)


class EngagementDownstreamMixin(
    OverwriteOutputMixin, WarehouseMixin, MapReduceJobTaskMixin, EventLogSelectionDownstreamMixin
):
    """Common parameters and base classes used to pass parameters through the engagement workflow"""

    # Required parameter
    date = luigi.DateParameter()

    # Override superclass to disable this parameter
    interval = None


# TODO: unit tests
class EngagementTask(EventLogSelectionMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    Process the event log and categorize user engagement with various types of content.

    This emits one record for each type of interaction. Note that this is loosely defined, for problems, for example, it
    will emit two records if the problem is correct (one for the "attempt" interaction and one for the "correct attempt"
    interaction).

    This task is intended to be run incrementally and populate a single Hive partition. Although time consuming to
    bootstrap the table, it results in a significantly cleaner workflow. It is much more clear what the success and
    failure conditions are for a task and the management of residual data is dramatically simplified. All of that said,
    Hadoop is not designed to operate like this and it would be significantly more efficient to process a range of dates
    at once. The choice was made to stick with the cleaner workflow since the steady-state is the same for both options
    - in general we will only be processing one day of data at a time.
    """

    # Required parameters
    date = luigi.DateParameter()
    output_root = luigi.Parameter()

    # Override superclass to disable these parameters
    interval = None

    def __init__(self, *args, **kwargs):
        super(EngagementTask, self).__init__(*args, **kwargs)

        self.interval = date_interval.Date.from_date(self.date)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')

        entity_id = None
        entity_type = None
        events = []
        if event_type == 'problem_check':
            if event_source != 'server':
                return

            entity_type = 'problem'
            if event_data.get('success', 'incorrect').lower() == 'correct':
                events.append('completed')

            events.append('attempted')
            entity_id = event_data.get('problem_id')
        elif event_type == 'play_video':
            entity_type = 'video'
            events.append('played')
            entity_id = event_data.get('id')
        elif event_type.startswith('edx.forum.'):
            entity_type = 'forum'
            if event_type == 'edx.forum.comment.created':
                events.append('commented')
            elif event_type == 'edx.forum.response.created':
                events.append('responded')
            elif event_type == 'edx.forum.thread.created':
                events.append('created')
            entity_id = event_data.get('commentable_id')

        if not entity_id or not entity_type:
            return

        for event in events:
            record = EngagementRecord(
                course_id=course_id,
                username=username,
                date=DateField.value_from_string(date_string),
                entity_type=entity_type,
                entity_id=entity_id,
                event=event,
                count=0
            )
            yield (record.to_string_tuple(), 1)

    def reducer(self, key, values):
        # Replace the count (which is hardcoded to 0 above, with the actual number of records in this group.
        yield ('\t'.join(key[:-1]), sum(values))

    def output(self):
        return get_target_from_url(self.output_root)

    def run(self):
        self.remove_output_on_overwrite()
        return super(EngagementTask, self).run()


class EngagementTableTask(BareHiveTableTask):

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'engagement'

    @property
    def columns(self):
        return EngagementRecord.to_hive_schema()


class EngagementPartitionTask(EngagementDownstreamMixin, HivePartitionTask):

    @property
    def partition_value(self):
        return self.date.isoformat()

    @property
    def hive_table_task(self):
        return EngagementTableTask(
            warehouse_path=self.warehouse_path
        )

    def requires(self):
        yield EngagementTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite=self.overwrite,
        )
        yield self.hive_table_task


class EngagementMysqlTask(EngagementDownstreamMixin, MysqlInsertTask):
    """
    This table is appended to every time this workflow is run, so it is expected to grow to be *very* large. For this
    reason, the records are stored in a clustered index allowing for very fast point queries and date range queries
    for individual users in particular courses.

    This allows us to rapidly generate activity charts over time for small numbers of users.
    """

    @property
    def table(self):
        return "engagement"

    def init_copy(self, connection):
        # clear only the data for this date!

        self.attempted_removal = True
        if self.overwrite:
            # first clear the appropriate rows from the luigi mysql marker table
            marker_table = self.output().marker_table  # side-effect: sets self.output_target if it's None
            try:
                query = "DELETE FROM {marker_table} where `update_id`='{update_id}'".format(
                    marker_table=marker_table,
                    update_id=self.update_id,
                )
                connection.cursor().execute(query)
            except mysql.connector.Error as excp:  # handle the case where the marker_table has yet to be created
                if excp.errno == errorcode.ER_NO_SUCH_TABLE:
                    pass
                else:
                    raise

            # Use "DELETE" instead of TRUNCATE since TRUNCATE forces an implicit commit before it executes which would
            # commit the currently open transaction before continuing with the copy.
            query = "DELETE FROM {table} WHERE date='{date}'".format(table=self.table, date=self.date.isoformat())
            connection.cursor().execute(query)

    @property
    def auto_primary_key(self):
        # We use a rather large compound primary key for this table to take advantage of the clustered index on read.
        return None

    @property
    def columns(self):
        return EngagementRecord.to_sql_schema()

    @property
    def keys(self):
        # Use a primary key since we will always be pulling these records by course_id, username ordered by date
        # This dramatically speeds up access times at the cost of write speed.

        # From: http://dev.mysql.com/doc/refman/5.6/en/innodb-restrictions.html

        # The InnoDB internal maximum key length is 3500 bytes, but MySQL itself restricts this to 3072 bytes. This
        # limit applies to the length of the combined index key in a multi-column index.

        # The total for this key is:
        #   course_id(255 characters * 3 bytes per utf8 char)
        #   username(30 characters * 3 bytes per utf8 char)
        #   date(3 bytes per DATE)
        #   entity_type(10 characters * 3 bytes per utf8 char)
        #   entity_id(255 characters * 3 bytes per utf8 char)
        #   event(30 characters * 3 bytes per utf8 char)
        #   count(4 bytes per INTEGER)

        # Total = 1747
        return [
            ('PRIMARY KEY', ['course_id', 'username', 'date', 'entity_type', 'entity_id', 'event'])
        ]

    @property
    def insert_source_task(self):
        return EngagementTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )


class EngagementVerticaTask(EngagementDownstreamMixin, VerticaCopyTask):

    @property
    def insert_source_task(self):
        return EngagementTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def table(self):
        return 'f_engagement'

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def columns(self):
        return EngagementRecord.to_sql_schema()


class OptionalVerticaMixin(object):
    """
    If a vertica connection is present, replicate the data there. Otherwise, don't require those insertion tasks.
    """

    vertica_schema = luigi.Parameter(default=None)
    vertica_credentials = luigi.Parameter(default=None)

    def __init__(self, *args, **kwargs):
        super(OptionalVerticaMixin, self).__init__(*args, **kwargs)

        if not self.vertica_credentials:
            self.vertica_credentials = get_config().get('vertica-export', 'credentials', None)

        if not self.vertica_schema:
            self.vertica_schema = get_config().get('vertica-export', 'schema', None)

        self.vertica_enabled = self.vertica_credentials and self.vertica_schema


class EngagementIntervalTask(
    MapReduceJobTaskMixin, EventLogSelectionDownstreamMixin, OverwriteOutputMixin, WarehouseMixin,
    OptionalVerticaMixin, luigi.WrapperTask
):
    """Compute engagement information over a range of dates and insert the results into Hive, Vertica and MySQL"""

    def requires(self):
        # Store the requirements like this, since downstream jobs will directly process the Hive partitions using
        # Map Reduce jobs. In order to get a pointer to the data to use as the input to the Map Reduce job, we need to
        # isolate the hive tasks from the MySQL and Vertica tasks.
        requirements = {
            'hive': [],
            'mysql': [],
            'vertica': []
        }
        for date in self.interval:
            requirements['hive'].append(
                EngagementPartitionTask(
                    date=date,
                    n_reduce_tasks=self.n_reduce_tasks,
                    warehouse_path=self.warehouse_path,
                    overwrite=self.overwrite,
                )
            )
            requirements['mysql'].append(
                EngagementMysqlTask(
                    date=date,
                    n_reduce_tasks=self.n_reduce_tasks,
                    warehouse_path=self.warehouse_path,
                    overwrite=self.overwrite,
                )
            )
            if self.vertica_enabled:
                requirements['vertica'].append(
                    EngagementVerticaTask(
                        date=date,
                        n_reduce_tasks=self.n_reduce_tasks,
                        warehouse_path=self.warehouse_path,
                        overwrite=self.overwrite,
                        schema=self.vertica_schema,
                        credentials=self.vertica_credentials,
                    )
                )

        return requirements

    def output(self):
        return [task.output() for task in self.requires().values()]


class SparseWeeklyStudentCourseEngagementTableTask(BareHiveTableTask):

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'sparse_course_engagement_weekly'

    @property
    def columns(self):
        return SparseWeeklyCourseEngagementRecord.to_hive_schema()


class SparseWeeklyStudentCourseEngagementPartitionTask(
    EngagementDownstreamMixin, OptionalVerticaMixin, HivePartitionTask
):

    @property
    def partition_value(self):
        return self.date.isoformat()

    @property
    def hive_table_task(self):
        return SparseWeeklyStudentCourseEngagementTableTask(
            warehouse_path=self.warehouse_path
        )

    def requires(self):
        yield SparseWeeklyStudentCourseEngagementTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite=self.overwrite,
            vertica_schema=self.vertica_schema,
            vertica_credentials=self.vertica_credentials,
        )
        yield self.hive_table_task


class SparseWeeklyStudentCourseEngagementTask(EngagementDownstreamMixin, OptionalVerticaMixin, MapReduceJobTask):
    """
    Store a sparse representation of student engagement with their courses on particular dates.

    Only emits a record if the user did something in the course on that particular day, this dramatically reduces the
    volume of data in this table and keeps it manageable.
    """

    output_root = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(SparseWeeklyStudentCourseEngagementTask, self).__init__(*args, **kwargs)

        start_date = self.date - datetime.timedelta(weeks=1)
        self.interval = date_interval.Custom(start_date, self.date)

    def requires_hadoop(self):
        return self.requires().requires()['hive']

    def mapper(self, line):
        record = EngagementRecord.from_tsv(line)
        yield ((record.course_id, record.username), line.strip())

    def reducer(self, key, lines):
        """Calculate counts for events corresponding to user and course in a given time period."""
        course_id, username = key

        output_record = SparseWeeklyCourseEngagementRecordBuilder()
        for line in lines:
            record = EngagementRecord.from_tsv(line)

            output_record.days_active.add(record.date)

            count = int(record.count)
            if record.entity_type == 'problem':
                if record.event == 'attempted':
                    output_record.problem_attempts += count
                    output_record.problems_attempted.add(record.entity_id)
                elif record.event == 'completed':
                    output_record.problems_completed.add(record.entity_id)
            elif record.entity_type == 'video':
                if record.event == 'played':
                    output_record.videos_played.add(record.entity_id)
            elif record.entity_type == 'forum':
                output_record.discussion_activity += count
            else:
                log.warn('Unrecognized entity type: %s', record.entity_type)

        yield SparseWeeklyCourseEngagementRecord(
            course_id,
            username,
            self.date,
            output_record.problem_attempts,
            len(output_record.problems_attempted),
            len(output_record.problems_completed),
            len(output_record.videos_played),
            output_record.discussion_activity,
            len(output_record.days_active)
        ).to_string_tuple()

    def output(self):
        return get_target_from_url(self.output_root)

    def requires(self):
        return EngagementIntervalTask(
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
            vertica_schema=self.vertica_schema,
            vertica_credentials=self.vertica_credentials,
        )


class SparseWeeklyCourseEngagementRecordBuilder(object):
    """Gather the data needed to emit a sparse weekly course engagmenet record"""

    def __init__(self):
        self.problem_attempts = 0
        self.problems_attempted = set()
        self.problems_completed = set()
        self.videos_played = set()
        self.discussion_activity = 0
        self.days_active = set()


class SparseWeeklyCourseEngagementRecord(Record):
    """
    Summarizes a user's engagement with a particular course on a particular day with simple counts of activity.
    """

    course_id = StringField()
    username = StringField()
    date = DateField()
    problem_attempts = IntegerField()
    problems_attempted = IntegerField()
    problems_completed = IntegerField()
    videos_played = IntegerField()
    discussion_activity = IntegerField()
    days_active = IntegerField()
