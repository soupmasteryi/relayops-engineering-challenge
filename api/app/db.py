import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://relayops:relayops@db:5432/relayops"
)

# open=False: the pool is opened/closed in the FastAPI lifespan (see main.py).
pool = ConnectionPool(
    DATABASE_URL,
    min_size=1,
    max_size=10,
    timeout=60,  # seconds to wait for a free connection before PoolTimeout
    kwargs={"row_factory": dict_row},
    check=ConnectionPool.check_connection,  # validate connections on checkout
    open=False,
)

# Read-only, REPEATABLE READ pool for /metrics, so several queries in one
# transaction share a single consistent snapshot. Kept separate so the
# isolation level never leaks onto connections used by the write endpoints.
metrics_pool = ConnectionPool(
    DATABASE_URL,
    min_size=1,
    max_size=2,
    timeout=60,
    kwargs={
        "row_factory": dict_row,
        "options": "-c default_transaction_isolation=repeatable\\ read "
        "-c default_transaction_read_only=on",
    },
    check=ConnectionPool.check_connection,
    open=False,
)
