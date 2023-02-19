# type: ignore
# Copyright 2023 Adap GmbH. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""SQLite based implemenation of server state."""
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from logging import ERROR
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID, uuid4

from flwr.common.logger import log
from flwr.proto.node_pb2 import Node
from flwr.proto.task_pb2 import Task, TaskIns, TaskRes
from flwr.proto.transport_pb2 import ClientMessage, ServerMessage
from flwr.server.utils.validator import validate_task_ins_or_res

from .state import State

SQL_CREATE_TABLE_NODE = """
CREATE TABLE IF NOT EXISTS node(
    id INTEGER UNIQUE
);
"""

SQL_CREATE_TABLE_TASK_INS = """
CREATE TABLE IF NOT EXISTS task_ins(
    task_id                 TEXT UNIQUE,
    group_id                TEXT,
    workload_id             TEXT,
    producer_anonymous      BOOLEAN,
    producer_node_id        INTEGER,
    consumer_anonymous      BOOLEAN,
    consumer_node_id        INTEGER,
    created_at              TEXT,
    delivered_at            TEXT,
    ttl                     TEXT,
    ancestry                TEXT,
    legacy_server_message   BLOB,
    legacy_client_message   BLOB
    -- FOREIGN KEY(task_id) REFERENCES node(id)
);
"""


SQL_CREATE_TABLE_TASK_RES = """
CREATE TABLE IF NOT EXISTS task_res(
    task_id                 TEXT UNIQUE,
    group_id                TEXT,
    workload_id             TEXT,
    producer_anonymous      BOOLEAN,
    producer_node_id        INTEGER,
    consumer_anonymous      BOOLEAN,
    consumer_node_id        INTEGER,
    created_at              TEXT,
    delivered_at            TEXT,
    ttl                     TEXT,
    ancestry                TEXT,
    legacy_server_message   BLOB,
    legacy_client_message   BLOB
    -- FOREIGN KEY(task_id) REFERENCES node(id)
);
"""

DICT_OR_TUPLE = Union[Tuple[Any], Dict[str, Any]]


class SqliteState(State):
    """SQLite based state implemenation."""

    def __init__(
        self,
        database_path: str,
    ) -> None:
        """Initialize an SqliteState.

        Parameters
        ----------
        database : (path-like object)
            The path to the database file to be opened. Pass ":memory:" to open
            a connectionto a database that is in RAM instead of on disk.
        """
        self.database_path = database_path
        self.conn: Optional[sqlite3.Connection] = None

    def initialize(self) -> List[Tuple[str]]:
        """Create tables if they don't exist yet."""
        self.conn = sqlite3.connect(self.database_path)
        self.conn.row_factory = dict_factory
        self.conn.set_trace_callback(lambda query: log(1, query))
        cur = self.conn.cursor()

        # Create each table if not exists queries
        cur.execute(SQL_CREATE_TABLE_TASK_INS)
        cur.execute(SQL_CREATE_TABLE_TASK_RES)
        cur.execute(SQL_CREATE_TABLE_NODE)

        res = cur.execute("SELECT name FROM sqlite_schema;")

        return res.fetchall()

    def _query(
        self,
        query: str,
        data: Optional[Union[List[DICT_OR_TUPLE], DICT_OR_TUPLE]] = None,
    ) -> List[Tuple[Any]]:
        if self.conn is None:
            raise Exception("State is not initialized.")

        if data is None:
            data = []

        # Cleanup whitespace to make the logs nicer
        query = re.sub(r"\s+", " ", query)

        try:
            with self.conn:
                if (
                    len(data) > 0
                    and (type(data) == tuple or type(data) == list)
                    and (type(data[0]) == tuple or type(data[0]) == dict)
                ):
                    rows = self.conn.executemany(query, data)
                else:
                    rows = self.conn.execute(query, data)

                # Extract results before commiting to support INSERT/UPDATE ... RETURNING style queries
                result = rows.fetchall()
        except KeyError as exc:
            log(ERROR, {"query": query, "data": data, "exception": exc})

        return result

    def store_task_ins(self, task_ins: TaskIns) -> Optional[UUID]:
        """Store one TaskIns.

        Usually, the Driver API calls this to schedule instructions.

        Stores the value of the task_ins in the state and, if successful, returns the
        task_id (UUID) of the task_ins. If, for any reason, storing the task_ins fails,
        `None` is returned.

        Constraints
        -----------
        If `task_ins.task.consumer.anonymous` is `True`, then
        `task_ins.task.consumer.node_id` MUST NOT be set (equal 0). Any implemenation
        may just override it with zero instead of validating.

        If `task_ins.task.consumer.anonymous` is `False`, then
        `task_ins.task.consumer.node_id` MUST be set (not 0)
        """
        # Validate task
        errors = validate_task_ins_or_res(task_ins)
        if any(errors):
            log(ERROR, errors)
            return None

        # Create and set task_id
        task_id = uuid4()
        task_ins.task_id = str(task_id)

        # Set created_at
        created_at: datetime = _now()
        task_ins.task.created_at = created_at.isoformat()

        # Set ttl
        ttl: datetime = created_at + timedelta(hours=24)
        task_ins.task.ttl = ttl.isoformat()

        # Store TaskIns
        data = (task_ins_to_dict(task_ins),)
        columns = ", ".join([f":{key}" for key in data[0].keys()])
        query = f"INSERT INTO task_ins VALUES({columns});"

        self._query(query, data)

        return task_id

    def get_task_ins(
        self, node_id: Optional[int], limit: Optional[int]
    ) -> List[TaskIns]:
        """Get TaskIns optionally filtered by node_id.

        Usually, the Fleet API calls this for Nodes planning to work on one or more
        TaskIns.

        Constraints
        -----------
        If `node_id` is not `None`, retrieve all TaskIns where

            1. the `task_ins.task.consumer.node_id` equals `node_id` AND
            2. the `task_ins.task.consumer.anonymous` equals `False` AND
            3. the `task_ins.task.delivered_at` equals `""`.

        If `node_id` is `None`, retrieve all TaskIns where the
        `task_ins.task.consumer.node_id` equals `0` and
        `task_ins.task.consumer.anonymous` is set to `True`.

        If `delivered_at` MUST BE set (not `""`) otherwise the TaskIns MUST not be in
        the result.

        If `limit` is not `None`, return, at most, `limit` number of `task_ins`. If
        `limit` is set, it has to be greater zero.
        """
        if limit is not None and limit < 1:
            raise AssertionError("`limit` must be >= 1")

        # Retrieve all anonymous Tasks
        if node_id == 0:
            return []

        data: Dict[str, Union[str, int]] = {}

        # Retrieve all anonymous Tasks
        if node_id is None:
            query = """
                SELECT task_id
                FROM task_ins
                WHERE consumer_anonymous == 1
                AND   consumer_node_id == 0
            """

        else:
            query = f"""
                SELECT task_id
                FROM task_ins
                WHERE consumer_anonymous == 0
                AND   consumer_node_id == :node_id
            """
            data["node_id"] = node_id

        if limit is not None:
            query += " LIMIT :limit"
            data["limit"] = limit

        query += ";"

        rows = self._query(query, data)

        if rows:
            # Prepare query
            task_ids = [row["task_id"] for row in rows]
            placeholders: str = ",".join([f":id_{i}" for i in range(len(task_ids))])
            query = f"""
                UPDATE task_ins
                SET delivered_at = :delivered_at
                WHERE task_id IN ({placeholders})
                RETURNING *;
            """

            # Prepare data for query
            delivered_at = _now().isoformat()
            data = {"delivered_at": delivered_at}
            for index, task_id in enumerate(task_ids):
                data[f"id_{index}"] = str(task_id)

            # Run query
            rows = self._query(query, data)

        result = [dict_to_task_ins(row) for row in rows]

        return result

    def store_task_res(self, task_res: TaskRes) -> Optional[UUID]:
        """Store one TaskRes.

        Usually, the Fleet API calls this for Nodes returning results.

        Stores the TaskRes and, if successful, returns the `task_id` (UUID) of
        the `task_res`. If storing the `task_res` fails, `None` is returned.

        Constraints
        -----------
        If `task_res.task.consumer.anonymous` is `True`, then
        `task_res.task.consumer.node_id` MUST NOT be set (equal 0). Any implemenation
        may just override it with zero instead of validating.

        If `task_res.task.consumer.anonymous` is `False`, then
        `task_res.task.consumer.node_id` MUST be set (not 0)
        """
        # Validate task
        errors = validate_task_ins_or_res(task_res)
        if any(errors):
            log(ERROR, errors)
            return None

        # Create and set task_id
        task_id = uuid4()
        task_res.task_id = str(task_id)

        # Set created_at
        created_at: datetime = _now()
        task_res.task.created_at = created_at.isoformat()

        # Set ttl
        ttl: datetime = created_at + timedelta(hours=24)
        task_res.task.ttl = ttl.isoformat()

        # Store TaskIns
        data = (task_res_to_dict(task_res),)
        columns = ", ".join([f":{key}" for key in data[0].keys()])
        query = f"INSERT INTO task_res VALUES({columns});"

        self._query(query, data)

        return task_id

    def get_task_res(self, task_ids: Set[UUID], limit: Optional[int]) -> List[TaskRes]:
        """Get TaskRes for task_ids.

        Usually, the Driver API calls this for Nodes planning to work on one or more
        TaskIns.

        Retrieves all TaskRes for the given `task_ids` and returns and empty list of
        none could be found.

        Constraints
        -----------
        If `limit` is not `None`, return, at most, `limit` number of TaskRes. The limit
        will only take effect if enough task_ids are in the set AND are currently
        available. If `limit` is set, it has to be greater zero.
        """
        if limit is not None and limit < 1:
            raise AssertionError("`limit` must be >= 1")

        # Retrieve all anonymous Tasks
        if len(task_ids) == 0:
            return []

        placeholders = ",".join([f":id_{i}" for i in range(len(task_ids))])
        query = f"""
            SELECT *
            FROM task_res
            WHERE ancestry IN ({placeholders})
            AND delivered_at = ""
        """

        if limit is not None:
            query += " LIMIT :limit"

        query += ";"

        data: Dict[str, Union[str, int]] = {"limit": limit}

        for index, task_id in enumerate(task_ids):
            data[f"id_{index}"] = str(task_id)

        rows = self._query(query, data)

        if rows:
            # Prepare query
            task_ids = [row["task_id"] for row in rows]
            placeholders = ",".join([f":id_{i}" for i in range(len(task_ids))])
            query = f"""
                UPDATE task_res
                SET delivered_at = :delivered_at
                WHERE task_id IN ({placeholders})
                RETURNING *;
            """

            # Prepare data for query
            delivered_at = _now().isoformat()
            data = {"delivered_at": delivered_at}
            for index, task_id in enumerate(task_ids):
                data[f"id_{index}"] = str(task_id)

            # Run query
            rows = self._query(query, data)

        result = [dict_to_task_res(row) for row in rows]
        return result

    def num_task_ins(self) -> int:
        """Number of task_ins in store.

        This includes delivered but not yet deleted task_ins.
        """
        query = "SELECT count(*) AS num FROM task_ins;"
        rows = self._query(query)
        result = rows[0]
        return result["num"]

    def num_task_res(self) -> int:
        """Number of task_res in store.

        This includes delivered but not yet deleted task_res.
        """
        query = "SELECT count(*) AS num FROM task_res;"
        rows = self._query(query)
        result: Dict[str, int] = rows[0]
        return result["num"]

    def delete_tasks(self, task_ids: Set[UUID]) -> None:
        """Delete all delivered TaskIns/TaskRes pairs."""
        ids = list(task_ids)
        if len(ids) == 0:
            return None

        placeholders = ",".join([f":id_{index}" for index in range(len(task_ids))])
        data = {f"id_{index}": str(task_id) for index, task_id in enumerate(task_ids)}

        # 1. Query: Delete task_ins which have a delivered task_res
        query_1 = f"""
            DELETE FROM task_ins
            WHERE delivered_at != ''
            AND task_id IN (
                SELECT ancestry
                FROM task_res
                WHERE ancestry IN ({placeholders})
                AND delivered_at != ''
            );
        """

        # 2. Query: Delete delivered task_res to be run after 1. Query
        query_2 = f"""
            DELETE FROM task_res
            WHERE ancestry IN ({placeholders})
            AND delivered_at != '';
        """

        if self.conn is None:
            raise Exception("State not intitialized")

        with self.conn:
            self.conn.execute(query_1, data)
            self.conn.execute(query_2, data)

    def register_node(self, node_id: int) -> None:
        """Store `node_id` in state."""
        query = "INSERT INTO node VALUES(:id);"
        self._query(query, (node_id,))

    def unregister_node(self, node_id: int) -> None:
        """Remove `node_id` from state."""
        query = "DELETE FROM node WHERE id = :id;"
        self._query(query, (node_id,))

    def get_nodes(self) -> Set[int]:
        """Retrieve all currently stored node IDs as a set."""
        query = "SELECT * FROM node;"
        rows = self._query(query)
        result: Set[int] = {row["id"] for row in rows}
        return result


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def dict_factory(cursor: sqlite3.Cursor, row: sqlite3.Row) -> Dict[str, Any]:
    """Used to turn SQLite results into dicts.

    Less efficent for retrival of large amounts of data but easier to
    use.
    """
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def task_ins_to_dict(task_msg: TaskIns) -> Dict[str, Any]:
    result = {
        "task_id": task_msg.task_id,
        "group_id": task_msg.group_id,
        "workload_id": task_msg.workload_id,
        "producer_anonymous": task_msg.task.producer.anonymous,
        "producer_node_id": task_msg.task.producer.node_id,
        "consumer_anonymous": task_msg.task.consumer.anonymous,
        "consumer_node_id": task_msg.task.consumer.node_id,
        "created_at": task_msg.task.created_at,
        "delivered_at": task_msg.task.delivered_at,
        "ttl": task_msg.task.ttl,
        "ancestry": ",".join(task_msg.task.ancestry),
        "legacy_server_message": task_msg.task.legacy_server_message.SerializeToString(),
        "legacy_client_message": None,
    }
    return result


def task_res_to_dict(task_msg: TaskRes) -> Dict[str, Any]:
    result = {
        "task_id": task_msg.task_id,
        "group_id": task_msg.group_id,
        "workload_id": task_msg.workload_id,
        "producer_anonymous": task_msg.task.producer.anonymous,
        "producer_node_id": task_msg.task.producer.node_id,
        "consumer_anonymous": task_msg.task.consumer.anonymous,
        "consumer_node_id": task_msg.task.consumer.node_id,
        "created_at": task_msg.task.created_at,
        "delivered_at": task_msg.task.delivered_at,
        "ttl": task_msg.task.ttl,
        "ancestry": ",".join(task_msg.task.ancestry),
        "legacy_server_message": None,
        "legacy_client_message": task_msg.task.legacy_client_message.SerializeToString(),
    }
    return result


def dict_to_task_ins(task_dict: Dict[str, Any]) -> TaskIns:
    """Turn task_dict into protobuf message."""
    server_message = ServerMessage()
    server_message.ParseFromString(task_dict["legacy_server_message"])

    result = TaskIns(
        task_id=task_dict["task_id"],
        group_id=task_dict["group_id"],
        workload_id=task_dict["workload_id"],
        task=Task(
            producer=Node(
                node_id=task_dict["producer_node_id"],
                anonymous=task_dict["producer_anonymous"],
            ),
            consumer=Node(
                node_id=task_dict["consumer_node_id"],
                anonymous=task_dict["consumer_anonymous"],
            ),
            created_at=task_dict["created_at"],
            delivered_at=task_dict["delivered_at"],
            ttl=task_dict["ttl"],
            ancestry=task_dict["ancestry"].split(","),
            legacy_server_message=server_message,
        ),
    )
    return result


def dict_to_task_res(task_dict: Dict[str, Any]) -> TaskRes:
    """Turn task_dict into protobuf message."""
    client_message = ClientMessage()
    client_message.ParseFromString(task_dict["legacy_client_message"])

    result = TaskRes(
        task_id=task_dict["task_id"],
        group_id=task_dict["group_id"],
        workload_id=task_dict["workload_id"],
        task=Task(
            producer=Node(
                node_id=task_dict["producer_node_id"],
                anonymous=task_dict["producer_anonymous"],
            ),
            consumer=Node(
                node_id=task_dict["consumer_node_id"],
                anonymous=task_dict["consumer_anonymous"],
            ),
            created_at=task_dict["created_at"],
            delivered_at=task_dict["delivered_at"],
            ttl=task_dict["ttl"],
            ancestry=task_dict["ancestry"].split(","),
            legacy_client_message=client_message,
        ),
    )
    return result
