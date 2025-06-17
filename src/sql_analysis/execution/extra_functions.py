

EXTRA_FUNCTIONS = [
    "CREATE OR REPLACE MACRO add_days(d, n) AS (d + n * INTERVAL '1 day')",
    "CREATE OR REPLACE MACRO add_months(d, n) AS (d + n * INTERVAL '1 month')",
    "CREATE OR REPLACE MACRO add_years(d, n) AS (d + n * INTERVAL '1 year')",
]