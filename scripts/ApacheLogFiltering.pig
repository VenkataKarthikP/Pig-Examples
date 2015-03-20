raw_log =
  -- load the logs into a sequence of one element tuples
  LOAD '/root/pig/input/mock_apache_pool-1-thread-1.data' AS line;
-- Parse log
parsed_log =
    FOREACH raw_log
    GENERATE
                FLATTEN (
                        REGEX_EXTRACT_ALL(
                                          line,
                                          '(\\S+) (\\S+) (\\S+) \\[(\\S+)\\] \\"(\\S+) (\\S+) (\\S+)\\" (\\d+) (\\d+) \\"(
\\S+)\\" \\"(.*)\\"')
                        )
    AS (
        ipaddress: chararray,
        blank0: chararray,
        blank1: chararray,
        date: chararray,
        call: chararray,
        page: chararray,
        protocal: chararray,
        errorcode: int,
        size: int,
        blank3: chararray,
        browser: chararray
    );

-- STORE parsed_log INTO '/root/pig/output/parselog/';
-- Register Jar, define ISOToUnix and convert to UnixTime
register '/usr/lib/pig/piggybank.jar' ;
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

date_filter = FOREACH parsed_log GENERATE ISOToUnix(date) AS unixTime:long;
 STORE date_filter INTO '/root/pig/output/parselogdate/';

filter =  FILTER  parsed_log BY (ipaddress == '197.188.107.201') OR (errorcode == 404);
