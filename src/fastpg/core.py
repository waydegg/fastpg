import asyncio
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any, Iterator, List, Optional, Sequence, Tuple, cast

import asyncpg
from asyncpg.pgproto.pgproto import UUID

from .utils import compile_query, compile_value


class Record(Mapping):
    __slots__ = "_rec"

    def __init__(self, rec: asyncpg.Record):
        self._rec = rec

    def _get(self, key: Any) -> Any:
        if type(key) == str:
            val = self._rec.get(key)
        else:
            val = self._rec[key]

        if type(val) == UUID:
            return str(val)
        return val

    def __getattr__(self, name: str) -> Any:
        return self._get(name)

    def keys(self) -> Tuple[str]:
        return tuple([k for k in self._rec.keys()])

    def values(self) -> Tuple[Any]:
        return tuple([self._get(k) for k in self._rec.keys()])

    def items(self) -> List[Tuple[str, Any]]:
        return list(zip(self.keys(), self.values()))

    def __len__(self) -> int:
        return len(self._rec)

    def __getitem__(self, key: Any) -> Any:
        return self._get(key)

    def __iter__(self) -> Iterator:
        return iter(self._rec.keys())


class Database:
    def __init__(
        self,
        dsn: str | None = None,
        *,
        user: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        force_rollback: bool = False,
        **kwargs,
    ):
        if dsn is None:
            assert user is not None, "Missing user (no DSN provided)"
            assert password is not None, "Missing password (no DSN provided)"
            assert host is not None, "Missing host (no DSN provided)"
            assert port is not None, "Missing port (no DSN provided)"
            dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.dsn = dsn
        self.kwargs = kwargs

        self._force_rollback = force_rollback

        self._pool: asyncpg.Pool | None = None
        self._global_connection: asyncpg.Connection | None = None

    async def connect(self):
        if self._force_rollback:
            assert self._global_connection is None, "Database already connected"
            self._global_connection = await asyncpg.connect(self.dsn, **self.kwargs)
            return

        assert self._pool is None, "Database already connected"
        self._pool = await asyncpg.create_pool(self.dsn, **self.kwargs)

    async def disconnect(self, timeout: float = 30):
        if self._force_rollback:
            assert self._global_connection is not None, "Database is not connected"
            await self._global_connection.close()
            self._global_connection = None
            return

        assert self._pool is not None, "Database is not connected"
        try:
            await asyncio.wait_for(self._pool.close(), timeout=timeout)
        except asyncio.TimeoutError:
            self._pool.terminate()
        self._pool = None

    @asynccontextmanager
    async def connection(self):
        if self._force_rollback:
            assert self._global_connection is not None, "Database is not connected"
            conn = Connection(self.dsn)
            conn._connection = self._global_connection
            async with conn.transaction(force_rollback=True):
                try:
                    yield conn
                finally:
                    return

        assert self._pool is not None, "Database is not connected"
        _conn = await self._pool.acquire()
        conn = Connection(self.dsn)
        conn._connection = _conn
        try:
            yield conn
        finally:
            await self._pool.release(conn._connection)

    async def execute(self, query: str, values: Optional[dict] = None):
        assert self._pool is not None, "Database is not connected"
        async with self.connection() as conn:
            await conn.execute(query, values)

    async def execute_many(self, query: str, values: List[dict]):
        assert self._pool is not None, "Database is not connected"
        async with self.connection() as conn:
            await conn.execute_many(query, values)

    async def fetch_val(self, query: str, values: Optional[dict] = None):
        assert self._pool is not None, "Database is not connected"
        async with self.connection() as conn:
            return await conn.fetch_val(query, values)

    async def fetch_one(self, query: str, values: Optional[dict] = None):
        assert self._pool is not None, "Database is not connected"
        async with self.connection() as conn:
            return await conn.fetch_one(query, values)

    async def fetch_many(self, query: str, values: Optional[dict] = None):
        assert self._pool is not None, "Database is not connected"
        async with self.connection() as conn:
            return await conn.fetch_many(query, values)

    async def copy_records_to_table(
        self,
        table_name: str,
        *,
        records: Sequence[Sequence],
        columns: Optional[List[str]] = None,
    ):
        assert self._pool is not None, "Database is not connected"
        async with self.connection() as conn:
            return await conn.copy_records_to_table(
                table_name, records=records, columns=columns
            )


class Connection:
    def __init__(
        self,
        dsn: str | None = None,
        *,
        user: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
    ):
        if dsn is None:
            assert user is not None, "Missing user (no DSN provided)"
            assert password is not None, "Missing password (no DSN provided)"
            assert host is not None, "Missing host (no DSN provided)"
            assert port is not None, "Missing port (no DSN provided)"
            dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.dsn = dsn
        self._connection: asyncpg.Connection | None = None

    async def connect(self):
        assert self._connection is None, "Connection already connected"
        self._connection = cast(asyncpg.Connection, await asyncpg.connect(self.dsn))

    async def close(self, *, timeout: float | None = None):
        assert self._connection is not None, "Connection is not connected"
        await self._connection.close(timeout=timeout)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def execute(self, query: str, values: Optional[dict] = None):
        assert self._connection is not None, "Connection is not acquired"
        compiled_query, ordered_values = compile_query(query, values)
        return await self._connection.execute(compiled_query, *ordered_values)

    async def execute_many(self, query: str, values: List[dict]):
        assert self._connection is not None, "Connection is not acquired"
        compiled_query, ordered_values = compile_query(query, values)
        await self._connection.executemany(compiled_query, ordered_values)

    async def fetch_val(self, query: str, values: Optional[dict] = None):
        assert self._connection is not None, "Connection is not acquired"
        compiled_query, ordered_values = compile_query(query, values)
        return await self._connection.fetchval(compiled_query, *ordered_values)

    # @overload
    # async def fetch_one(
    #     self, query: str, values: Optional[dict] = None, cast: Mapping = Dict
    # ) -> Record:
    #     ...
    #
    # @overload
    # async def fetch_one(
    #     self, query: str, values: Optional[dict] = None, cast: None = None
    # ) -> Record | None:
    #     ...

    async def fetch_one(
        self, query: str, values: Optional[dict] = None, cast: Mapping | None = None
    ):
        assert self._connection is not None, "Connection is not acquired"
        compiled_query, ordered_values = compile_query(query, values)
        rec = await self._connection.fetchrow(compiled_query, *ordered_values)

        return Record(rec) if rec else None

    async def fetch_many(self, query: str, values: Optional[dict] = None):
        assert self._connection is not None, "Connection is not acquired"
        compiled_query, ordered_values = compile_query(query, values)
        recs = await self._connection.fetch(compiled_query, *ordered_values)

        return [Record(rec) for rec in recs]

    async def copy_records_to_table(
        self,
        table_name: str,
        *,
        records: Sequence[Sequence[Any]],
        columns: Optional[List[str]] = None,
    ):
        assert self._connection is not None, "Connection is not acquired"
        await self._connection.copy_records_to_table(
            table_name,
            records=[[compile_value(v) for v in rec] for rec in records],
            columns=columns,
        )

    @asynccontextmanager
    async def transaction(self, *, force_rollback: bool = False):
        assert self._connection is not None, "Connection is not connected"
        transaction = self._connection.transaction()
        await transaction.start()
        try:
            yield transaction
        except:
            await transaction.rollback()
            raise
        else:
            if force_rollback:
                await transaction.rollback()
                return

            await transaction.commit()
