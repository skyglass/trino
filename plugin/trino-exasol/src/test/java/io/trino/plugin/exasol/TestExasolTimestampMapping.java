package io.trino.plugin.exasol;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.exasol.TestingExasolServer.TEST_SCHEMA;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;

@Isolated
public class TestExasolTimestampMapping
        extends AbstractTestQueryFramework
{
    private TestingExasolServer exasolServer;

    private final ZoneId jvmZone = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    private static final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    @BeforeAll
    public void setUp()
    {
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1932, 4, 1);
        verify(jvmZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay()).isEmpty());

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        verify(vilnius.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay()).isEmpty());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        verify(vilnius.getRules().getValidOffsets(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1)).size() == 2);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        exasolServer = closeAfterClass(new TestingExasolServer());
        return ExasolQueryRunner.builder(exasolServer).build();
    }

    @Test
    void testTimestamp()
    {
        testTimestamp(UTC);
        testTimestamp(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Exasol system zone is
        testTimestamp(vilnius);
        testTimestamp(kathmandu);
        testTimestamp(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    /**
     * Read {@code TIMESTAMP}s inserted by Exasol as Trino {@code TIMESTAMP WITH TIME ZONE}s
     */
    @Test
    public void testTimestampFromExasol()
    {
        testTimestampFromExasol(UTC);
        testTimestampFromExasol(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testTimestampFromExasol(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testTimestampFromExasol(ZoneId.of("Asia/Kathmandu"));
        testTimestampFromExasol(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    @Test
    public void testUnsupportedTimestampValues()
    {
        try (TestTable table = new TestTable(exasolServer::execute, "tpch.test_unsupported_timestamp", "(col TIMESTAMP)")) {

            // Too early
            assertExasolQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (TIMESTAMP '0000-12-31 23:59:59.999999')",
                    "data exception - invalid date value");

            // Too late
            assertExasolQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (TIMESTAMP '10000-01-01 00:00:00.000000')",
                    "data exception - invalid character value for cast");

            // Precision > 9
            assertExasolQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (TIMESTAMP '2024-01-01 12:34:56.1234567890')",
                    "data exception - invalid character value for cast");
        }
    }

    private void assertExasolQueryFails(String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> exasolServer.execute(sql))
                .cause()
                .hasMessageContaining(expectedMessage);
    }


    private void testTimestampFromExasol(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        // Formatter that handles variable fractional seconds (up to 9 digits)
        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .toFormatter();

        // Helper function to convert local timestamp string to expected UTC TIMESTAMP string
        Function<String, String> toExpectedUtcTimestamp = (localTimestamp) -> {
            LocalDateTime ldt = LocalDateTime.parse(localTimestamp, formatter);
            Instant instant = ldt.atZone(sessionZone).toInstant();
            String formatted = instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
            return "TIMESTAMP '" + formatted + " UTC'";
        };

        SqlDataTypeTest test = SqlDataTypeTest.create()
                // after epoch (MySQL's timestamp type doesn't support values <= epoch)
                .addRoundTrip("timestamp with local time zone", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("2019-03-18 10:01:17.987"))

                // time doubled in JVM zone (commented out in your original, add if needed)
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("2018-10-28 01:33:17.456"))
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("2018-10-28 03:33:33.333"))
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("1970-01-01 00:13:42.000"))
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("2018-04-01 02:13:55.123"))
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("2018-03-25 03:17:17.000"))
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("1986-01-01 00:13:07.000"))

                .addRoundTrip("timestamp(6) with local time zone", "TIMESTAMP '2019-03-18 10:01:17.987654'", createTimestampWithTimeZoneType(6), toExpectedUtcTimestamp.apply("2019-03-18 10:01:17.987654"))
                .addRoundTrip("timestamp(6) with local time zone", "TIMESTAMP '2018-10-28 01:33:17.456789'", createTimestampWithTimeZoneType(6), toExpectedUtcTimestamp.apply("2018-10-28 01:33:17.456789"))
                .addRoundTrip("timestamp(6) with local time zone", "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampWithTimeZoneType(6), toExpectedUtcTimestamp.apply("2018-10-28 03:33:33.333333"))
                .addRoundTrip("timestamp(6) with local time zone", "TIMESTAMP '1970-01-01 00:13:42.000000'", createTimestampWithTimeZoneType(6), toExpectedUtcTimestamp.apply("1970-01-01 00:13:42.000000"))
                .addRoundTrip("timestamp(6) with local time zone", "TIMESTAMP '2018-04-01 02:13:55.123456'", createTimestampWithTimeZoneType(6), toExpectedUtcTimestamp.apply("2018-04-01 02:13:55.123456"))
                .addRoundTrip("timestamp(6) with local time zone", "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampWithTimeZoneType(6), toExpectedUtcTimestamp.apply("2018-03-25 03:17:17.000000"))
                .addRoundTrip("timestamp(6) with local time zone", "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampWithTimeZoneType(6), toExpectedUtcTimestamp.apply("1986-01-01 00:13:07.000000"))

                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp(0) with local time zone", "TIMESTAMP '1970-01-01 00:00:01'", createTimestampWithTimeZoneType(0), toExpectedUtcTimestamp.apply("1970-01-01 00:00:01"))
                .addRoundTrip("timestamp(1) with local time zone", "TIMESTAMP '1970-01-01 00:00:01.1'", createTimestampWithTimeZoneType(1), toExpectedUtcTimestamp.apply("1970-01-01 00:00:01.1"))
                .addRoundTrip("timestamp(1) with local time zone", "TIMESTAMP '1970-01-01 00:00:01.9'", createTimestampWithTimeZoneType(1), toExpectedUtcTimestamp.apply("1970-01-01 00:00:01.9"))
                .addRoundTrip("timestamp(2) with local time zone", "TIMESTAMP '1970-01-01 00:00:01.12'", createTimestampWithTimeZoneType(2), toExpectedUtcTimestamp.apply("1970-01-01 00:00:01.12"))
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '1970-01-01 00:00:01.123'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("1970-01-01 00:00:01.123"))
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '1970-01-01 00:00:01.999'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("1970-01-01 00:00:01.999"))
                .addRoundTrip("timestamp(4) with local time zone", "TIMESTAMP '1970-01-01 00:00:01.1234'", createTimestampWithTimeZoneType(4), toExpectedUtcTimestamp.apply("1970-01-01 00:00:01.1234"))
                .addRoundTrip("timestamp(5) with local time zone", "TIMESTAMP '1970-01-01 00:00:01.12345'", createTimestampWithTimeZoneType(5), toExpectedUtcTimestamp.apply("1970-01-01 00:00:01.12345"))
                .addRoundTrip("timestamp(1) with local time zone", "TIMESTAMP '2020-09-27 12:34:56.1'", createTimestampWithTimeZoneType(1), toExpectedUtcTimestamp.apply("2020-09-27 12:34:56.1"))
                .addRoundTrip("timestamp(1) with local time zone", "TIMESTAMP '2020-09-27 12:34:56.9'", createTimestampWithTimeZoneType(1), toExpectedUtcTimestamp.apply("2020-09-27 12:34:56.9"))
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '2020-09-27 12:34:56.123'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("2020-09-27 12:34:56.123"))
                .addRoundTrip("timestamp(3) with local time zone", "TIMESTAMP '2020-09-27 12:34:56.999'", createTimestampWithTimeZoneType(3), toExpectedUtcTimestamp.apply("2020-09-27 12:34:56.999"))
                .addRoundTrip("timestamp(6) with local time zone", "TIMESTAMP '2020-09-27 12:34:56.123456'", createTimestampWithTimeZoneType(6), toExpectedUtcTimestamp.apply("2020-09-27 12:34:56.123456"));

        test.execute(getQueryRunner(), session, exasolCreateAndInsert("tpch.test_timestamp"));
    }


    private void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp", "NULL", createTimestampType(3), "CAST(NULL AS TIMESTAMP)")
                .addRoundTrip("timestamp", "TIMESTAMP '2013-03-11 17:30:15.123'", createTimestampType(3), "TIMESTAMP '2013-03-11 17:30:15.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2013-03-11 17:30:15.123456'", createTimestampType(6), "TIMESTAMP '2013-03-11 17:30:15.123456'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2013-03-11 17:30:15.123456789'", createTimestampType(9), "TIMESTAMP '2013-03-11 17:30:15.123456789'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2016-08-19 19:28:05.0'", createTimestampType(1), "TIMESTAMP '2016-08-19 19:28:05.0'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '2016-08-19 19:28:05.01'", createTimestampType(2), "TIMESTAMP '2016-08-19 19:28:05.01'")
                .addRoundTrip("timestamp", "TIMESTAMP '3030-03-03 12:34:56.123'", createTimestampType(3), "TIMESTAMP '3030-03-03 12:34:56.123'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '3030-03-03 12:34:56.12345'", createTimestampType(5), "TIMESTAMP '3030-03-03 12:34:56.12345'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '3030-03-03 12:34:56.123456789'", createTimestampType(9), "TIMESTAMP '3030-03-03 12:34:56.123456789'")
                .addRoundTrip("timestamp", "TIMESTAMP '3030-03-03 12:34:56.123'", createTimestampType(3), "TIMESTAMP '3030-03-03 12:34:56.123'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '3030-03-03 12:34:56.12345'", createTimestampType(5), "TIMESTAMP '3030-03-03 12:34:56.12345'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '3030-03-03 12:34:56.123456789'", createTimestampType(9), "TIMESTAMP '3030-03-03 12:34:56.123456789'")
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2017-07-01'", createTimestampType(0), "TIMESTAMP '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2017-01-01'", createTimestampType(0), "TIMESTAMP '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01'", createTimestampType(0), "TIMESTAMP '1970-01-01'") // change forward at midnight in JVM
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1983-04-01'", createTimestampType(0), "TIMESTAMP '1983-04-01'") // change forward at midnight in Vilnius
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1983-10-01'", createTimestampType(0), "TIMESTAMP '1983-10-01'") // change backward at midnight in Vilnius
                .addRoundTrip("timestamp(0)", "TIMESTAMP '9999-12-31'", createTimestampType(0), "TIMESTAMP '9999-12-31'") // max value in Exasol
                .execute(getQueryRunner(), session, exasolCreateAndInsert(TEST_SCHEMA + "." + "test_timestamp"));
    }

    private DataSetup exasolCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(exasolServer::execute, tableNamePrefix);
    }
}

