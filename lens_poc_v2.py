from RedShiftConnection import RedShiftConn
import properties as cfg
import etl_log_config

import datetime
import json
import pandas

print("lens-poc Start ")
etl_log_config.debuglogger.warning("lens_poc.py Start:  ")

con = RedShiftConn.getConnectionObj()


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super(DatetimeEncoder, obj).default(obj)
        except TypeError:
            return str(obj)


def redshift_to_csv_by_query(query_var):
    date = json.dumps(datetime.datetime.now(), cls=DatetimeEncoder)
    etl_log_config.debuglogger.warning("lens_poc.py - Redshift TO CSV data copy redshift_to_csv_by_query() start for query --> " + query_var + ' at ' + str(date))
    print("lens_poc.py - Redshift TO CSV data copy redshift_to_csv_by_query() start for query ", query_var)
    file_name = 'data/' + query_var + '.csv'
    try:
        is_header_needed = True
        for chunk in pandas.read_sql_query(cfg.lens_poc[query_var], con, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=50000):
            if is_header_needed:
                chunk.to_csv(file_name, sep=',', encoding='utf-8', mode="a", index=False)
                is_header_needed = False
            else:
                chunk.to_csv(file_name, sep=',', encoding='utf-8', mode="a", index=False, header=False)

        date = json.dumps(datetime.datetime.now(), cls=DatetimeEncoder)
        etl_log_config.debuglogger.warning(
            "lens_poc.py - Redshift TO CSV data copy redshift_to_csv_by_query() for query --> " + query_var + " - Ends at --> " + str(date))
        print("lens_poc.py - Redshift TO CSV data copy redshift_to_csv_by_query() Ends for query -->  ", query_var)
    except Exception as e:
        etl_log_config.debuglogger.error("lens_poc.py - Error Occurred in redshift_to_csv_by_query() : for query --> " + query_var + " - error: " + str(e))


def main():
    date = json.dumps(datetime.datetime.now(), cls=DatetimeEncoder)
    etl_log_config.debuglogger.warning("lens_poc.py - Redshift TO CSV data copy main() start at --> " + str(date))
    print("lens_poc.py - Redshift TO CSV data copy main() start at ", str(date))
    try:

        redshift_to_csv_by_query('q1_retail')
        redshift_to_csv_by_query('q2_retail_aligned_sales')
        redshift_to_csv_by_query('q3_non-retail-unaligned-sales')
        redshift_to_csv_by_query('q4_non_retail_aligned_sales')

        date = json.dumps(datetime.datetime.now(), cls=DatetimeEncoder)
        etl_log_config.debuglogger.warning("lens_poc.py - Redshift TO CSV data copy main() completed at --> " + str(date))
        print("lens_poc.py - Redshift TO CSV data copy main() Ends at ", str(date))

    except Exception as e:
        etl_log_config.debuglogger.error("lens_poc.py - Redshift TO CSV data copy main() Ends with error --> " + str(e))
        con.close()


if __name__ == '__main__':
    main()
    con.close()
etl_log_config.debuglogger.warning("lens_poc.py END:  ")