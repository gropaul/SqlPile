from src.sql_analysis.add_3rd_party.add_sql_storm import add_sql_storm_stack_overflow, add_sql_storm_tpc, \
    add_sql_storm_job
from src.sql_analysis.add_3rd_party.add_tpc import add_tpc

TPC_SF = 5

def add_all_benchmarks():
    add_tpc('tpc-h')
    add_tpc('tpc-ds')
    add_sql_storm_stack_overflow()
    add_sql_storm_tpc('tpc-h')
    add_sql_storm_tpc('tpc-ds')
    add_sql_storm_job()


if __name__ == "__main__":
    add_all_benchmarks()