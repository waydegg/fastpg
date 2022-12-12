import asyncio
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any, Iterable, Iterator, List, Optional, Tuple

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

    def __len__(self) -> int:  # type: ignore
        return len(self._rec)

    def __getitem__(self, key: Any) -> Any:
        return self._get(key)

    def __iter__(self) -> Iterator:
        return iter(self._rec.keys())


class Database:
    def __init__(self, dsn: str, *, force_rollback: bool = False):
        self.dsn = dsn

        self._force_rollback = force_rollback

        self._pool: asyncpg.Pool | None = None
        self._global_connection: Connection | None = None

    async def connect(self):
        if self._force_rollback:
            assert self._global_connection is None, "Database already connected"
            self._global_connection = await asyncpg.connect(self.dsn)
            return

        assert self._pool is None, "Database already connected"
        self._pool = await asyncpg.create_pool(self.dsn)

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
            conn = Connection(self._global_connection)
            async with conn.transaction(force_rollback=True):
                try:
                    yield conn
                finally:
                    return

        assert self._pool is not None, "Database is not connected"
        _conn = await self._pool.acquire()
        conn = Connection(_conn)
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
        records: List[Iterable],
        columns: Optional[List[str]] = None,
    ):
        assert self._pool is not None, "Database is not connected"
        async with self.connection() as conn:
            return await conn.copy_records_to_table(
                table_name, records=records, columns=columns
            )


class Connection:
    def __init__(self, connection):
        self._connection = connection

    async def close(self, *, timeout: float | None = None):
        await self._connection.close(timeout=timeout)

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

    async def fetch_one(self, query: str, values: Optional[dict] = None):
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
        records: List[Iterable],
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
