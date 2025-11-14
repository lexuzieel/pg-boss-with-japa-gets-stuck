import EventEmitter from "node:events";
import assert, { notStrictEqual } from "node:assert";
import { randomUUID } from "node:crypto";
import { serializeError } from "serialize-error";
import { CronExpressionParser } from "cron-parser";
import { setTimeout } from "node:timers/promises";
import pg from "pg";

//#region src/plans.ts
const DEFAULT_SCHEMA = "pgboss";
const MIGRATE_RACE_MESSAGE = "division by zero";
const CREATE_RACE_MESSAGE = "already exists";
const FIFTEEN_MINUTES = 900;
const FORTEEN_DAYS = 3600 * 24 * 14;
const SEVEN_DAYS = 3600 * 24 * 7;
const JOB_STATES = Object.freeze({
	created: "created",
	retry: "retry",
	active: "active",
	completed: "completed",
	cancelled: "cancelled",
	failed: "failed"
});
const QUEUE_POLICIES = Object.freeze({
	standard: "standard",
	short: "short",
	singleton: "singleton",
	stately: "stately",
	exclusive: "exclusive"
});
const QUEUE_DEFAULTS = {
	expire_seconds: FIFTEEN_MINUTES,
	retention_seconds: FORTEEN_DAYS,
	deletion_seconds: SEVEN_DAYS,
	retry_limit: 2,
	retry_delay: 0,
	warning_queued: 0,
	retry_backoff: false,
	partition: false
};
const COMMON_JOB_TABLE = "job_common";
function create(schema, version, options) {
	return locked(schema, [
		options?.createSchema ? createSchema(schema) : "",
		createEnumJobState(schema),
		createTableVersion(schema),
		createTableQueue(schema),
		createTableSchedule(schema),
		createTableSubscription(schema),
		createTableJob(schema),
		createPrimaryKeyJob(schema),
		createTableJobCommon(schema, COMMON_JOB_TABLE),
		createQueueFunction(schema),
		deleteQueueFunction(schema),
		insertVersion(schema, version)
	]);
}
function createSchema(schema) {
	return `CREATE SCHEMA IF NOT EXISTS ${schema}`;
}
function createEnumJobState(schema) {
	return `
    CREATE TYPE ${schema}.job_state AS ENUM (
      '${JOB_STATES.created}',
      '${JOB_STATES.retry}',
      '${JOB_STATES.active}',
      '${JOB_STATES.completed}',
      '${JOB_STATES.cancelled}',
      '${JOB_STATES.failed}'
    )
  `;
}
function createTableVersion(schema) {
	return `
    CREATE TABLE ${schema}.version (
      version int primary key,
      cron_on timestamp with time zone
    )
  `;
}
function createTableQueue(schema) {
	return `
    CREATE TABLE ${schema}.queue (
      name text NOT NULL,
      policy text NOT NULL,
      retry_limit int NOT NULL,
      retry_delay int NOT NULL,
      retry_backoff bool NOT NULL,
      retry_delay_max int,
      expire_seconds int NOT NULL,
      retention_seconds int NOT NULL,
      deletion_seconds int NOT NULL,
      dead_letter text REFERENCES ${schema}.queue (name) CHECK (dead_letter IS DISTINCT FROM name),
      partition bool NOT NULL,
      table_name text NOT NULL,
      deferred_count int NOT NULL default 0,
      queued_count int NOT NULL default 0,
      warning_queued int NOT NULL default 0,
      active_count int NOT NULL default 0,
      total_count int NOT NULL default 0,
      singletons_active text[],
      monitor_on timestamp with time zone,
      maintain_on timestamp with time zone,
      created_on timestamp with time zone not null default now(),
      updated_on timestamp with time zone not null default now(),
      PRIMARY KEY (name)
    )
  `;
}
function createTableSchedule(schema) {
	return `
    CREATE TABLE ${schema}.schedule (
      name text REFERENCES ${schema}.queue ON DELETE CASCADE,
      key text not null DEFAULT '',
      cron text not null,
      timezone text,
      data jsonb,
      options jsonb,
      created_on timestamp with time zone not null default now(),
      updated_on timestamp with time zone not null default now(),
      PRIMARY KEY (name, key)
    )
  `;
}
function createTableSubscription(schema) {
	return `
    CREATE TABLE ${schema}.subscription (
      event text not null,
      name text not null REFERENCES ${schema}.queue ON DELETE CASCADE,
      created_on timestamp with time zone not null default now(),
      updated_on timestamp with time zone not null default now(),
      PRIMARY KEY(event, name)
    )
  `;
}
function createTableJob(schema) {
	return `
    CREATE TABLE ${schema}.job (
      id uuid not null default gen_random_uuid(),
      name text not null,
      priority integer not null default(0),
      data jsonb,
      state ${schema}.job_state not null default '${JOB_STATES.created}',
      retry_limit integer not null default ${QUEUE_DEFAULTS.retry_limit},
      retry_count integer not null default 0,
      retry_delay integer not null default ${QUEUE_DEFAULTS.retry_delay},
      retry_backoff boolean not null default ${QUEUE_DEFAULTS.retry_backoff},
      retry_delay_max integer,
      expire_seconds int not null default ${QUEUE_DEFAULTS.expire_seconds},
      deletion_seconds int not null default ${QUEUE_DEFAULTS.deletion_seconds},
      singleton_key text,
      singleton_on timestamp without time zone,
      start_after timestamp with time zone not null default now(),
      created_on timestamp with time zone not null default now(),
      started_on timestamp with time zone,
      completed_on timestamp with time zone,
      keep_until timestamp with time zone NOT NULL default now() + interval '${QUEUE_DEFAULTS.retention_seconds}',
      output jsonb,
      dead_letter text,
      policy text
    ) PARTITION BY LIST (name)
  `;
}
const JOB_COLUMNS_MIN = "id, name, data, expire_seconds as \"expireInSeconds\"";
const JOB_COLUMNS_ALL = `${JOB_COLUMNS_MIN},
  policy,
  state,
  priority,
  retry_limit as "retryLimit",
  retry_count as "retryCount",
  retry_delay as "retryDelay",
  retry_backoff as "retryBackoff",
  retry_delay_max as "retryDelayMax",
  start_after as "startAfter",
  started_on as "startedOn",
  singleton_key as "singletonKey",
  singleton_on as "singletonOn",
  deletion_seconds as "deleteAfterSeconds",
  created_on as "createdOn",
  completed_on as "completedOn",
  keep_until as "keepUntil",
  dead_letter as "deadLetter",
  output
`;
function createTableJobCommon(schema, table) {
	const format = (command) => command.replaceAll(".job", `.${table}`) + ";";
	return `
    CREATE TABLE ${schema}.${table} (LIKE ${schema}.job INCLUDING GENERATED INCLUDING DEFAULTS);
    ${format(createPrimaryKeyJob(schema))}
    ${format(createQueueForeignKeyJob(schema))}
    ${format(createQueueForeignKeyJobDeadLetter(schema))}
    ${format(createIndexJobPolicyShort(schema))}
    ${format(createIndexJobPolicySingleton(schema))}
    ${format(createIndexJobPolicyStately(schema))}
    ${format(createIndexJobPolicyExclusive(schema))}
    ${format(createIndexJobThrottle(schema))}
    ${format(createIndexJobFetch(schema))}

    ALTER TABLE ${schema}.job ATTACH PARTITION ${schema}.${table} DEFAULT;
  `;
}
function createQueueFunction(schema) {
	return `
    CREATE FUNCTION ${schema}.create_queue(queue_name text, options jsonb)
    RETURNS VOID AS
    $$
    DECLARE
      tablename varchar := CASE WHEN options->>'partition' = 'true'
                            THEN 'j' || encode(sha224(queue_name::bytea), 'hex')
                            ELSE '${COMMON_JOB_TABLE}'
                            END;
      queue_created_on timestamptz;
    BEGIN

      WITH q as (
        INSERT INTO ${schema}.queue (
          name,
          policy,
          retry_limit,
          retry_delay,
          retry_backoff,
          retry_delay_max,
          expire_seconds,
          retention_seconds,
          deletion_seconds,
          warning_queued,
          dead_letter,
          partition,
          table_name
        )
        VALUES (
          queue_name,
          options->>'policy',
          COALESCE((options->>'retryLimit')::int, ${QUEUE_DEFAULTS.retry_limit}),
          COALESCE((options->>'retryDelay')::int, ${QUEUE_DEFAULTS.retry_delay}),
          COALESCE((options->>'retryBackoff')::bool, ${QUEUE_DEFAULTS.retry_backoff}),
          (options->>'retryDelayMax')::int,
          COALESCE((options->>'expireInSeconds')::int, ${QUEUE_DEFAULTS.expire_seconds}),
          COALESCE((options->>'retentionSeconds')::int, ${QUEUE_DEFAULTS.retention_seconds}),
          COALESCE((options->>'deleteAfterSeconds')::int, ${QUEUE_DEFAULTS.deletion_seconds}),
          COALESCE((options->>'warningQueueSize')::int, ${QUEUE_DEFAULTS.warning_queued}),
          options->>'deadLetter',
          COALESCE((options->>'partition')::bool, ${QUEUE_DEFAULTS.partition}),
          tablename
        )
        ON CONFLICT DO NOTHING
        RETURNING created_on
      )
      SELECT created_on into queue_created_on from q;

      IF queue_created_on IS NULL OR options->>'partition' IS DISTINCT FROM 'true' THEN
        RETURN;
      END IF;

      EXECUTE format('CREATE TABLE ${schema}.%I (LIKE ${schema}.job INCLUDING DEFAULTS)', tablename);
      
      EXECUTE format('${formatPartitionCommand(createPrimaryKeyJob(schema))}', tablename);
      EXECUTE format('${formatPartitionCommand(createQueueForeignKeyJob(schema))}', tablename);
      EXECUTE format('${formatPartitionCommand(createQueueForeignKeyJobDeadLetter(schema))}', tablename);

      EXECUTE format('${formatPartitionCommand(createIndexJobFetch(schema))}', tablename);
      EXECUTE format('${formatPartitionCommand(createIndexJobThrottle(schema))}', tablename);
      
      IF options->>'policy' = 'short' THEN
        EXECUTE format('${formatPartitionCommand(createIndexJobPolicyShort(schema))}', tablename);
      ELSIF options->>'policy' = 'singleton' THEN
        EXECUTE format('${formatPartitionCommand(createIndexJobPolicySingleton(schema))}', tablename);
      ELSIF options->>'policy' = 'stately' THEN
        EXECUTE format('${formatPartitionCommand(createIndexJobPolicyStately(schema))}', tablename);
      ELSIF options->>'policy' = 'exclusive' THEN
        EXECUTE format('${formatPartitionCommand(createIndexJobPolicyExclusive(schema))}', tablename);
      END IF;

      EXECUTE format('ALTER TABLE ${schema}.%I ADD CONSTRAINT cjc CHECK (name=%L)', tablename, queue_name);
      EXECUTE format('ALTER TABLE ${schema}.job ATTACH PARTITION ${schema}.%I FOR VALUES IN (%L)', tablename, queue_name);
    END;
    $$
    LANGUAGE plpgsql;
  `;
}
function formatPartitionCommand(command) {
	return command.replace(".job", ".%1$I").replace("job_i", "%1$s_i").replaceAll("'", "''");
}
function deleteQueueFunction(schema) {
	return `
    CREATE FUNCTION ${schema}.delete_queue(queue_name text)
    RETURNS VOID AS
    $$
    DECLARE
      v_table varchar;
      v_partition bool;
    BEGIN
      SELECT table_name, partition
      FROM ${schema}.queue
      WHERE name = queue_name
      INTO v_table, v_partition;

      IF v_partition THEN
        EXECUTE format('DROP TABLE IF EXISTS ${schema}.%I', v_table);
      ELSE
        EXECUTE format('DELETE FROM ${schema}.%I WHERE name = %L', v_table, queue_name);
      END IF;

      DELETE FROM ${schema}.queue WHERE name = queue_name;
    END;
    $$
    LANGUAGE plpgsql;
  `;
}
function createQueue(schema, name, options) {
	return locked(schema, `SELECT ${schema}.create_queue('${name}', '${JSON.stringify(options)}'::jsonb)`, "create-queue");
}
function deleteQueue(schema, name) {
	return locked(schema, `SELECT ${schema}.delete_queue('${name}')`, "delete-queue");
}
function createPrimaryKeyJob(schema) {
	return `ALTER TABLE ${schema}.job ADD PRIMARY KEY (name, id)`;
}
function createQueueForeignKeyJob(schema) {
	return `ALTER TABLE ${schema}.job ADD CONSTRAINT q_fkey FOREIGN KEY (name) REFERENCES ${schema}.queue (name) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED`;
}
function createQueueForeignKeyJobDeadLetter(schema) {
	return `ALTER TABLE ${schema}.job ADD CONSTRAINT dlq_fkey FOREIGN KEY (dead_letter) REFERENCES ${schema}.queue (name) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED`;
}
function createIndexJobPolicyShort(schema) {
	return `CREATE UNIQUE INDEX job_i1 ON ${schema}.job (name, COALESCE(singleton_key, '')) WHERE state = '${JOB_STATES.created}' AND policy = '${QUEUE_POLICIES.short}'`;
}
function createIndexJobPolicySingleton(schema) {
	return `CREATE UNIQUE INDEX job_i2 ON ${schema}.job (name, COALESCE(singleton_key, '')) WHERE state = '${JOB_STATES.active}' AND policy = '${QUEUE_POLICIES.singleton}'`;
}
function createIndexJobPolicyStately(schema) {
	return `CREATE UNIQUE INDEX job_i3 ON ${schema}.job (name, state, COALESCE(singleton_key, '')) WHERE state <= '${JOB_STATES.active}' AND policy = '${QUEUE_POLICIES.stately}'`;
}
function createIndexJobThrottle(schema) {
	return `CREATE UNIQUE INDEX job_i4 ON ${schema}.job (name, singleton_on, COALESCE(singleton_key, '')) WHERE state <> '${JOB_STATES.cancelled}' AND singleton_on IS NOT NULL`;
}
function createIndexJobFetch(schema) {
	return `CREATE INDEX job_i5 ON ${schema}.job (name, start_after) INCLUDE (priority, created_on, id) WHERE state < '${JOB_STATES.active}'`;
}
function createIndexJobPolicyExclusive(schema) {
	return `CREATE UNIQUE INDEX job_i6 ON ${schema}.job (name, COALESCE(singleton_key, '')) WHERE state <= '${JOB_STATES.active}' AND policy = '${QUEUE_POLICIES.exclusive}'`;
}
function trySetQueueMonitorTime(schema, queues, seconds) {
	return trySetQueueTimestamp(schema, queues, "monitor_on", seconds);
}
function trySetQueueDeletionTime(schema, queues, seconds) {
	return trySetQueueTimestamp(schema, queues, "maintain_on", seconds);
}
function trySetCronTime(schema, seconds) {
	return trySetTimestamp(schema, "cron_on", seconds);
}
function trySetTimestamp(schema, column, seconds) {
	return `
    UPDATE ${schema}.version
    SET ${column} = now()
    WHERE EXTRACT( EPOCH FROM (now() - COALESCE(${column}, now() - interval '1 week') ) ) > ${seconds}
    RETURNING true
  `;
}
function trySetQueueTimestamp(schema, queues, column, seconds) {
	return `
    UPDATE ${schema}.queue
    SET ${column} = now()
    WHERE name IN(${getQueueInClause(queues)})
      AND EXTRACT( EPOCH FROM (now() - COALESCE(${column}, now() - interval '1 week') ) ) > ${seconds}
    RETURNING name    
  `;
}
function updateQueue(schema, { deadLetter } = {}) {
	return `
    WITH options as (SELECT $2::jsonb as data)
    UPDATE ${schema}.queue SET
      retry_limit = COALESCE((o.data->>'retryLimit')::int, retry_limit),
      retry_delay = COALESCE((o.data->>'retryDelay')::int, retry_delay),
      retry_backoff = COALESCE((o.data->>'retryBackoff')::bool, retry_backoff),
      retry_delay_max = CASE WHEN o.data ? 'retryDelayMax'
        THEN (o.data->>'retryDelayMax')::int
        ELSE retry_delay_max END,
      expire_seconds = COALESCE((o.data->>'expireInSeconds')::int, expire_seconds),
      retention_seconds = COALESCE((o.data->>'retentionSeconds')::int, retention_seconds),
      deletion_seconds = COALESCE((o.data->>'deleteAfterSeconds')::int, deletion_seconds),
      warning_queued = COALESCE((o.data->>'warningQueueSize')::int, warning_queued),
      ${deadLetter === void 0 ? "" : `dead_letter = CASE WHEN '${deadLetter}' IS DISTINCT FROM dead_letter THEN '${deadLetter}' ELSE dead_letter END,`}
      updated_on = now()
    FROM options o
    WHERE name = $1
  `;
}
function getQueues(schema, names) {
	return `
    SELECT 
      q.name,
      q.policy,
      q.retry_limit as "retryLimit",
      q.retry_delay as "retryDelay",
      q.retry_backoff as "retryBackoff",
      q.retry_delay_max as "retryDelayMax",
      q.expire_seconds as "expireInSeconds",
      q.retention_seconds as "retentionSeconds",
      q.deletion_seconds as "deleteAfterSeconds",
      q.partition,
      q.dead_letter as "deadLetter",
      q.deferred_count as "deferredCount",
      q.warning_queued as "warningQueueSize",
      q.queued_count as "queuedCount",
      q.active_count as "activeCount",
      q.total_count as "totalCount",
      q.singletons_active as "singletonsActive",
      q.table_name as "table",
      q.created_on as "createdOn",
      q.updated_on as "updatedOn"
    FROM ${schema}.queue q
    ${names ? `WHERE q.name IN (${names.map((i) => `'${i}'`)})` : ""}
   `;
}
function deleteJobsById(schema, table) {
	return `
    WITH results as (
      DELETE FROM ${schema}.${table}
      WHERE name = $1
        AND id IN (SELECT UNNEST($2::uuid[]))        
      RETURNING 1
    )
    SELECT COUNT(*) from results
  `;
}
function deleteQueuedJobs(schema, table) {
	return `DELETE from ${schema}.${table} WHERE name = $1 and state < '${JOB_STATES.active}'`;
}
function deleteStoredJobs(schema, table) {
	return `DELETE from ${schema}.${table} WHERE name = $1 and state > '${JOB_STATES.active}'`;
}
function truncateTable(schema, table) {
	return `TRUNCATE ${schema}.${table}`;
}
function deleteAllJobs(schema, table) {
	return `DELETE from ${schema}.${table} WHERE name = $1`;
}
function getSchedules(schema) {
	return `SELECT * FROM ${schema}.schedule`;
}
function getSchedulesByQueue(schema) {
	return `SELECT * FROM ${schema}.schedule WHERE name = $1 AND COALESCE(key, '') = $2`;
}
function schedule(schema) {
	return `
    INSERT INTO ${schema}.schedule (name, key, cron, timezone, data, options)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (name, key) DO UPDATE SET
      cron = EXCLUDED.cron,
      timezone = EXCLUDED.timezone,
      data = EXCLUDED.data,
      options = EXCLUDED.options,
      updated_on = now()
  `;
}
function unschedule(schema) {
	return `
    DELETE FROM ${schema}.schedule
    WHERE name = $1
      AND COALESCE(key, '') = $2
  `;
}
function subscribe(schema) {
	return `
    INSERT INTO ${schema}.subscription (event, name)
    VALUES ($1, $2)
    ON CONFLICT (event, name) DO UPDATE SET
      event = EXCLUDED.event,
      name = EXCLUDED.name,
      updated_on = now()
  `;
}
function unsubscribe(schema) {
	return `
    DELETE FROM ${schema}.subscription
    WHERE event = $1 and name = $2
  `;
}
function getQueuesForEvent(schema) {
	return `
    SELECT name FROM ${schema}.subscription
    WHERE event = $1
  `;
}
function getTime() {
	return "SELECT round(date_part('epoch', now()) * 1000) as time";
}
function getVersion(schema) {
	return `SELECT version from ${schema}.version`;
}
function setVersion(schema, version) {
	return `UPDATE ${schema}.version SET version = '${version}'`;
}
function versionTableExists(schema) {
	return `SELECT to_regclass('${schema}.version') as name`;
}
function insertVersion(schema, version) {
	return `INSERT INTO ${schema}.version(version) VALUES ('${version}')`;
}
function fetchNextJob({ schema, table, name, policy, limit, includeMetadata, priority = true, ignoreStartAfter = false, ignoreSingletons = null }) {
	const singletonFetch = limit > 1 && (policy === QUEUE_POLICIES.singleton || policy === QUEUE_POLICIES.stately);
	const cte = singletonFetch ? "grouped" : "next";
	return `
    WITH next as (
      SELECT id ${singletonFetch ? ", singleton_key" : ""}
      FROM ${schema}.${table}
      WHERE name = '${name}'
        AND state < '${JOB_STATES.active}'
        ${ignoreStartAfter ? "" : "AND start_after < now()"}
        ${ignoreSingletons != null && ignoreSingletons?.length > 0 ? `AND singleton_key NOT IN (${ignoreSingletons.map((i) => `'${i}'`).join()})` : ""}
      ORDER BY ${priority ? "priority desc, " : ""}created_on, id
      LIMIT ${limit}
      FOR UPDATE SKIP LOCKED
    )
    ${singletonFetch ? ", grouped as ( SELECT id, row_number() OVER (PARTITION BY singleton_key) FROM next)" : ""}
    UPDATE ${schema}.${table} j SET
      state = '${JOB_STATES.active}',
      started_on = now(),
      retry_count = CASE WHEN started_on IS NOT NULL THEN retry_count + 1 ELSE retry_count END
    FROM ${cte}
    WHERE name = '${name}' AND j.id = ${cte}.id
      ${singletonFetch ? ` AND ${cte}.row_number = 1` : ""}
    RETURNING j.${includeMetadata ? JOB_COLUMNS_ALL : JOB_COLUMNS_MIN}      
  `;
}
function completeJobs(schema, table) {
	return `
    WITH results AS (
      UPDATE ${schema}.${table}
      SET completed_on = now(),
        state = '${JOB_STATES.completed}',
        output = $3::jsonb
      WHERE name = $1
        AND id IN (SELECT UNNEST($2::uuid[]))
        AND state = '${JOB_STATES.active}'
      RETURNING *
    )
    SELECT COUNT(*) FROM results
  `;
}
function cancelJobs(schema, table) {
	return `
    WITH results as (
      UPDATE ${schema}.${table}
      SET completed_on = now(),
        state = '${JOB_STATES.cancelled}'
      WHERE name = $1
        AND id IN (SELECT UNNEST($2::uuid[]))
        AND state < '${JOB_STATES.completed}'
      RETURNING 1
    )
    SELECT COUNT(*) from results
  `;
}
function resumeJobs(schema, table) {
	return `
    WITH results as (
      UPDATE ${schema}.${table}
      SET completed_on = NULL,
        state = '${JOB_STATES.created}'
      WHERE name = $1
        AND id IN (SELECT UNNEST($2::uuid[]))
        AND state = '${JOB_STATES.cancelled}'
      RETURNING 1
    )
    SELECT COUNT(*) from results
  `;
}
function insertJobs(schema, { table, name, returnId = true }) {
	return `
    INSERT INTO ${schema}.${table} (
      id,
      name,
      data,
      priority,
      start_after,
      singleton_key,
      singleton_on,
      expire_seconds,
      deletion_seconds,
      keep_until,
      retry_limit,
      retry_delay,
      retry_backoff,
      retry_delay_max,
      policy,
      dead_letter
    )
    SELECT
      COALESCE(id, gen_random_uuid()) as id,
      '${name}' as name,
      data,
      COALESCE(priority, 0) as priority,
      j.start_after,
      "singletonKey",
      CASE
        WHEN "singletonSeconds" IS NOT NULL THEN 'epoch'::timestamp + '1s'::interval * ("singletonSeconds" * floor(( date_part('epoch', now()) + COALESCE("singletonOffset",0)) / "singletonSeconds" ))
        ELSE NULL
        END as singleton_on,
      COALESCE("expireInSeconds", q.expire_seconds) as expire_seconds,
      COALESCE("deleteAfterSeconds", q.deletion_seconds) as deletion_seconds,
      j.start_after + (COALESCE("retentionSeconds", q.retention_seconds) * interval '1s') as keep_until,
      COALESCE("retryLimit", q.retry_limit) as retry_limit,
      COALESCE("retryDelay", q.retry_delay) as retry_delay,
      COALESCE("retryBackoff", q.retry_backoff, false) as retry_backoff,
      COALESCE("retryDelayMax", q.retry_delay_max) as retry_delay_max,
      q.policy,
      q.dead_letter
    FROM (
      SELECT *,
        CASE
          WHEN right("startAfter", 1) = 'Z' THEN CAST("startAfter" as timestamp with time zone)
          ELSE now() + CAST(COALESCE("startAfter",'0') as interval)
          END as start_after
      FROM json_to_recordset($1::json) as x (
        id uuid,
        priority integer,
        data jsonb,
        "startAfter" text,
        "retryLimit" integer,
        "retryDelay" integer,
        "retryDelayMax" integer,
        "retryBackoff" boolean,
        "singletonKey" text,
        "singletonSeconds" integer,
        "singletonOffset" integer,
        "expireInSeconds" integer,
        "deleteAfterSeconds" integer,
        "retentionSeconds" integer
      ) 
    ) j
    JOIN ${schema}.queue q ON q.name = '${name}'
    ON CONFLICT DO NOTHING
    ${returnId ? "RETURNING id" : ""}
  `;
}
function failJobsById(schema, table) {
	return failJobs(schema, table, `name = $1 AND id IN (SELECT UNNEST($2::uuid[])) AND state < '${JOB_STATES.completed}'`, "$3::jsonb");
}
function failJobsByTimeout(schema, table, queues) {
	return locked(schema, failJobs(schema, table, `state = '${JOB_STATES.active}'
            AND (started_on + expire_seconds * interval '1s') < now()
            AND name IN (${getQueueInClause(queues)})`, "'{ \"value\": { \"message\": \"job timed out\" } }'::jsonb"), table + "failJobsByTimeout");
}
function failJobs(schema, table, where, output) {
	return `
    WITH deleted_jobs AS (
      DELETE FROM ${schema}.${table}
      WHERE ${where}
      RETURNING *
    ),
    retried_jobs AS (
      INSERT INTO ${schema}.${table} (
        id,
        name,
        priority,
        data,
        state,
        retry_limit,
        retry_count,
        retry_delay,
        retry_backoff,
        retry_delay_max,
        start_after,
        started_on,
        singleton_key,
        singleton_on,
        expire_seconds,
        deletion_seconds,
        created_on,
        completed_on,
        keep_until,
        policy,
        output,
        dead_letter
      )
      SELECT
        id,
        name,
        priority,
        data,
        CASE
          WHEN retry_count < retry_limit THEN '${JOB_STATES.retry}'::${schema}.job_state
          ELSE '${JOB_STATES.failed}'::${schema}.job_state
          END as state,
        retry_limit,
        retry_count,
        retry_delay,
        retry_backoff,
        retry_delay_max,
        CASE WHEN retry_count = retry_limit THEN start_after
             WHEN NOT retry_backoff THEN now() + retry_delay * interval '1'
             ELSE now() + LEAST(
               retry_delay_max,
               retry_delay + (
                2 ^ LEAST(16, retry_count + 1) / 2 +
                2 ^ LEAST(16, retry_count + 1) / 2 * random()
               )
             ) * interval '1s'
        END as start_after,
        started_on,
        singleton_key,
        singleton_on,
        expire_seconds,
        deletion_seconds,
        created_on,
        CASE WHEN retry_count < retry_limit THEN NULL ELSE now() END as completed_on,
        keep_until,
        policy,
        ${output},
        dead_letter
      FROM deleted_jobs
      ON CONFLICT DO NOTHING
      RETURNING *
    ),
    failed_jobs as (
      INSERT INTO ${schema}.${table} (
        id,
        name,
        priority,
        data,
        state,
        retry_limit,
        retry_count,
        retry_delay,
        retry_backoff,
        retry_delay_max,
        start_after,
        started_on,
        singleton_key,
        singleton_on,
        expire_seconds,
        deletion_seconds,
        created_on,
        completed_on,
        keep_until,
        policy,
        output,
        dead_letter
      )
      SELECT
        id,
        name,
        priority,
        data,
        '${JOB_STATES.failed}'::${schema}.job_state as state,
        retry_limit,
        retry_count,
        retry_delay,
        retry_backoff,
        retry_delay_max,
        start_after,
        started_on,
        singleton_key,
        singleton_on,
        expire_seconds,
        deletion_seconds,
        created_on,
        now() as completed_on,
        keep_until,
        policy,
        ${output},
        dead_letter
      FROM deleted_jobs
      WHERE id NOT IN (SELECT id from retried_jobs)
      RETURNING *
    ),
    results as (
      SELECT * FROM retried_jobs
      UNION ALL
      SELECT * FROM failed_jobs
    ),
    dlq_jobs as (
      INSERT INTO ${schema}.job (name, data, output, retry_limit, retry_backoff, retry_delay, keep_until, deletion_seconds)
      SELECT
        r.dead_letter,
        data,
        output,
        q.retry_limit,
        q.retry_backoff,
        q.retry_delay,
        now() + q.retention_seconds * interval '1s',
        q.deletion_seconds
      FROM results r
        JOIN ${schema}.queue q ON q.name = r.dead_letter
      WHERE state = '${JOB_STATES.failed}'
    )
    SELECT COUNT(*) FROM results
  `;
}
function deletion(schema, table, queues) {
	return locked(schema, `
    DELETE FROM ${schema}.${table}
    WHERE name IN (${getQueueInClause(queues)})
      AND
      (
        completed_on + deletion_seconds * interval '1s' < now()
        OR
        (state < '${JOB_STATES.active}' AND keep_until < now())
      )        
  `, table + "deletion");
}
function retryJobs(schema, table) {
	return `
    WITH results as (
      UPDATE ${schema}.job
      SET state = '${JOB_STATES.retry}',
        retry_limit = retry_limit + 1
      WHERE name = $1
        AND id IN (SELECT UNNEST($2::uuid[]))
        AND state = '${JOB_STATES.failed}'
      RETURNING 1
    )
    SELECT COUNT(*) from results
  `;
}
function getQueueStats(schema, table, queues) {
	return `
    SELECT
        name, 
        (count(*) FILTER (WHERE start_after > now()))::int as "deferredCount",
        (count(*) FILTER (WHERE state < '${JOB_STATES.active}'))::int as "queuedCount",
        (count(*) FILTER (WHERE state = '${JOB_STATES.active}'))::int as "activeCount",
        count(*)::int as "totalCount",
        array_agg(singleton_key) FILTER (WHERE policy IN ('${QUEUE_POLICIES.singleton}','${QUEUE_POLICIES.stately}') AND state = '${JOB_STATES.active}') as "singletonsActive"
      FROM ${schema}.${table}
      WHERE name IN (${getQueueInClause(queues)})
      GROUP BY 1
  `;
}
function cacheQueueStats(schema, table, queues) {
	return locked(schema, `
    WITH stats AS (${getQueueStats(schema, table, queues)})
    UPDATE ${schema}.queue SET
      deferred_count = "deferredCount",
      queued_count = "queuedCount",
      active_count = "activeCount",
      total_count = "totalCount",
      singletons_active = "singletonsActive"
    FROM stats
      WHERE queue.name = stats.name
    RETURNING
      queue.name,
      "queuedCount",
      warning_queued as "warningQueueSize"
  `, "queue-stats");
}
function locked(schema, query, key) {
	if (Array.isArray(query)) query = query.join(";\n");
	return `
    BEGIN;
    SET LOCAL lock_timeout = 30000;
    SET LOCAL idle_in_transaction_session_timeout = 30000;
    ${advisoryLock(schema, key)};
    ${query};
    COMMIT;
  `;
}
function advisoryLock(schema, key) {
	return `SELECT pg_advisory_xact_lock(
      ('x' || encode(sha224((current_database() || '.pgboss.${schema}${key || ""}')::bytea), 'hex'))::bit(64)::bigint
  )`;
}
function assertMigration(schema, version) {
	return `SELECT version::int/(version::int-${version}) from ${schema}.version`;
}
function getJobById(schema, table) {
	return `SELECT ${JOB_COLUMNS_ALL} FROM ${schema}.${table} WHERE name = $1 AND id = $2`;
}
function getQueueInClause(queues) {
	return queues.map((i) => `'${i}'`).join(",");
}

//#endregion
//#region src/attorney.ts
const POLICY = {
	MAX_EXPIRATION_HOURS: 24,
	MIN_POLLING_INTERVAL_MS: 500,
	MAX_RETENTION_DAYS: 365
};
function assertObjectName(value, name = "Name") {
	assert(/^[\w.-]+$/.test(value), `${name} can only contain alphanumeric characters, underscores, hyphens, or periods`);
}
function validateQueueArgs(config = {}) {
	assert(!("deadLetter" in config) || config.deadLetter === null || typeof config.deadLetter === "string", "deadLetter must be a string");
	if (config.deadLetter) assertObjectName(config.deadLetter, "deadLetter");
	validateRetryConfig(config);
	validateExpirationConfig(config);
	validateRetentionConfig(config);
	validateDeletionConfig(config);
}
function checkSendArgs(args) {
	let name, data, options;
	if (typeof args[0] === "string") {
		name = args[0];
		data = args[1];
		assert(typeof data !== "function", "send() cannot accept a function as the payload.  Did you intend to use work()?");
		options = args[2];
	} else if (typeof args[0] === "object") {
		assert(args.length === 1, "send object API only accepts 1 argument");
		const job = args[0];
		assert(job, "boss requires all jobs to have a name");
		name = job.name;
		data = job.data;
		options = job.options;
	}
	options = options || {};
	assert(name, "boss requires all jobs to have a queue name");
	assert(typeof options === "object", "options should be an object");
	options = { ...options };
	assert(!("priority" in options) || Number.isInteger(options.priority), "priority must be an integer");
	options.priority = options.priority || 0;
	options.startAfter = options.startAfter instanceof Date && typeof options.startAfter.toISOString === "function" ? options.startAfter.toISOString() : +options.startAfter > 0 ? "" + options.startAfter : typeof options.startAfter === "string" ? options.startAfter : void 0;
	validateRetryConfig(options);
	validateExpirationConfig(options);
	validateRetentionConfig(options);
	validateDeletionConfig(options);
	return {
		name,
		data,
		options
	};
}
function checkWorkArgs(name, args) {
	let options, callback;
	assert(name, "missing job name");
	if (args.length === 1) {
		callback = args[0];
		options = {};
	} else if (args.length > 1) {
		options = args[0] || {};
		callback = args[1];
	}
	assert(typeof callback === "function", "expected callback to be a function");
	assert(typeof options === "object", "expected config to be an object");
	options = { ...options };
	applyPollingInterval(options);
	assert(!("batchSize" in options) || Number.isInteger(options.batchSize) && options.batchSize >= 1, "batchSize must be an integer > 0");
	assert(!("includeMetadata" in options) || typeof options.includeMetadata === "boolean", "includeMetadata must be a boolean");
	assert(!("priority" in options) || typeof options.priority === "boolean", "priority must be a boolean");
	options.batchSize = options.batchSize || 1;
	return {
		options,
		callback
	};
}
function checkFetchArgs(name, options) {
	assert(name, "missing queue name");
	assert(!("batchSize" in options) || Number.isInteger(options.batchSize) && options.batchSize >= 1, "batchSize must be an integer > 0");
	assert(!("includeMetadata" in options) || typeof options.includeMetadata === "boolean", "includeMetadata must be a boolean");
	assert(!("priority" in options) || typeof options.priority === "boolean", "priority must be a boolean");
	assert(!("ignoreStartAfter" in options) || typeof options.ignoreStartAfter === "boolean", "ignoreStartAfter must be a boolean");
	options.batchSize = options.batchSize || 1;
}
function getConfig(value) {
	assert(value && (typeof value === "object" || typeof value === "string"), "configuration assert: string or config object is required to connect to postgres");
	const config = typeof value === "string" ? { connectionString: value } : { ...value };
	config.schedule = "schedule" in config ? config.schedule : true;
	config.supervise = "supervise" in config ? config.supervise : true;
	config.migrate = "migrate" in config ? config.migrate : true;
	config.createSchema = "createSchema" in config ? config.createSchema : true;
	applySchemaConfig(config);
	applyOpsConfig(config);
	applyScheduleConfig(config);
	validateWarningConfig(config);
	return config;
}
function applySchemaConfig(config) {
	if (config.schema) assertPostgresObjectName(config.schema);
	config.schema = config.schema || DEFAULT_SCHEMA;
}
function validateWarningConfig(config) {
	assert(!("warningQueueSize" in config) || config.warningQueueSize >= 1, "configuration assert: warningQueueSize must be at least 1");
	assert(!("warningSlowQuerySeconds" in config) || config.warningSlowQuerySeconds >= 1, "configuration assert: warningSlowQuerySeconds must be at least 1");
}
function assertPostgresObjectName(name) {
	assert(typeof name === "string", "Name must be a string");
	assert(name.length <= 50, "Name cannot exceed 50 characters");
	assert(!/\W/.test(name), "Name can only contain alphanumeric characters or underscores");
	assert(!/^\d/.test(name), "Name cannot start with a number");
}
function assertQueueName(name) {
	assert(name, "Name is required");
	assert(typeof name === "string", "Name must be a string");
	assertObjectName(name);
}
function assertKey(key) {
	if (!key) return;
	assert(typeof key === "string", "Key must be a string");
	assertObjectName(key, "Key");
}
function validateRetentionConfig(config) {
	assert(!("retentionSeconds" in config) || config.retentionSeconds >= 1, "configuration assert: retentionSeconds must be at least every second");
}
function validateExpirationConfig(config) {
	assert(!("expireInSeconds" in config) || config.expireInSeconds >= 1, "configuration assert: expireInSeconds must be at least every second");
	assert(!config.expireInSeconds || config.expireInSeconds / 60 / 60 < POLICY.MAX_EXPIRATION_HOURS, `configuration assert: expiration cannot exceed ${POLICY.MAX_EXPIRATION_HOURS} hours`);
}
function validateRetryConfig(config) {
	assert(!("retryDelay" in config) || Number.isInteger(config.retryDelay) && config.retryDelay >= 0, "retryDelay must be an integer >= 0");
	assert(!("retryLimit" in config) || Number.isInteger(config.retryLimit) && config.retryLimit >= 0, "retryLimit must be an integer >= 0");
	assert(!("retryBackoff" in config) || config.retryBackoff === true || config.retryBackoff === false, "retryBackoff must be either true or false");
	assert(!("retryDelayMax" in config) || config.retryDelayMax === null || config.retryBackoff === true, "retryDelayMax can only be set if retryBackoff is true");
	assert(!("retryDelayMax" in config) || config.retryDelayMax === null || Number.isInteger(config.retryDelayMax) && config.retryDelayMax >= 0, "retryDelayMax must be an integer >= 0");
}
function applyPollingInterval(config) {
	assert(!("pollingIntervalSeconds" in config) || config.pollingIntervalSeconds >= POLICY.MIN_POLLING_INTERVAL_MS / 1e3, `configuration assert: pollingIntervalSeconds must be at least every ${POLICY.MIN_POLLING_INTERVAL_MS}ms`);
	config.pollingInterval = "pollingIntervalSeconds" in config ? config.pollingIntervalSeconds * 1e3 : 2e3;
}
function applyOpsConfig(config) {
	assert(!("superviseIntervalSeconds" in config) || config.superviseIntervalSeconds >= 1, "configuration assert: superviseIntervalSeconds must be at least every second");
	config.superviseIntervalSeconds = config.superviseIntervalSeconds || 60;
	assert(config.superviseIntervalSeconds / 60 / 60 <= POLICY.MAX_EXPIRATION_HOURS, `configuration assert: superviseIntervalSeconds cannot exceed ${POLICY.MAX_EXPIRATION_HOURS} hours`);
	assert(!("maintenanceIntervalSeconds" in config) || config.maintenanceIntervalSeconds >= 1, "configuration assert: maintenanceIntervalSeconds must be at least every second");
	config.maintenanceIntervalSeconds = config.maintenanceIntervalSeconds || POLICY.MAX_EXPIRATION_HOURS * 60 * 60;
	assert(config.maintenanceIntervalSeconds / 60 / 60 <= POLICY.MAX_EXPIRATION_HOURS, `configuration assert: maintenanceIntervalSeconds cannot exceed ${POLICY.MAX_EXPIRATION_HOURS} hours`);
	assert(!("monitorIntervalSeconds" in config) || config.monitorIntervalSeconds >= 1, "configuration assert: monitorIntervalSeconds must be at least every second");
	config.monitorIntervalSeconds = config.monitorIntervalSeconds || 60;
	assert(config.monitorIntervalSeconds / 60 / 60 <= POLICY.MAX_EXPIRATION_HOURS, `configuration assert: monitorIntervalSeconds cannot exceed ${POLICY.MAX_EXPIRATION_HOURS} hours`);
	assert(!("queueCacheIntervalSeconds" in config) || config.queueCacheIntervalSeconds >= 1, "configuration assert: queueCacheIntervalSeconds must be at least every second");
	config.queueCacheIntervalSeconds = config.queueCacheIntervalSeconds || 60;
	assert(config.queueCacheIntervalSeconds / 60 / 60 <= POLICY.MAX_EXPIRATION_HOURS, `configuration assert: queueCacheIntervalSeconds cannot exceed ${POLICY.MAX_EXPIRATION_HOURS} hours`);
}
function validateDeletionConfig(config) {
	assert(!("deleteAfterSeconds" in config) || config.deleteAfterSeconds >= 1, "configuration assert: deleteAfterSeconds must be at least every second");
}
function applyScheduleConfig(config) {
	assert(!("clockMonitorIntervalSeconds" in config) || config.clockMonitorIntervalSeconds >= 1 && config.clockMonitorIntervalSeconds <= 600, "configuration assert: clockMonitorIntervalSeconds must be between 1 second and 10 minutes");
	config.clockMonitorIntervalSeconds = config.clockMonitorIntervalSeconds || 600;
	assert(!("cronMonitorIntervalSeconds" in config) || config.cronMonitorIntervalSeconds >= 1 && config.cronMonitorIntervalSeconds <= 45, "configuration assert: cronMonitorIntervalSeconds must be between 1 and 45 seconds");
	config.cronMonitorIntervalSeconds = config.cronMonitorIntervalSeconds || 30;
	assert(!("cronWorkerIntervalSeconds" in config) || config.cronWorkerIntervalSeconds >= 1 && config.cronWorkerIntervalSeconds <= 45, "configuration assert: cronWorkerIntervalSeconds must be between 1 and 45 seconds");
	config.cronWorkerIntervalSeconds = config.cronWorkerIntervalSeconds || 5;
}

//#endregion
//#region src/migrationStore.ts
function flatten(schema, commands, version) {
	commands.unshift(assertMigration(schema, version));
	commands.push(setVersion(schema, version));
	return locked(schema, commands);
}
function rollback(schema, version, migrations) {
	migrations = migrations || getAll(schema);
	const result = migrations.find((i) => i.version === version);
	assert(result, `Version ${version} not found.`);
	return flatten(schema, result.uninstall || [], result.previous);
}
function next(schema, version, migrations) {
	migrations = migrations || getAll(schema);
	const result = migrations.find((i) => i.previous === version);
	assert(result, `Version ${version} not found.`);
	return flatten(schema, result.install, result.version);
}
function migrate(schema, version, migrations) {
	migrations = migrations || getAll(schema);
	const result = migrations.filter((i) => i.previous >= version).sort((a, b) => a.version - b.version).reduce((acc, i) => {
		acc.install = acc.install.concat(i.install);
		acc.version = i.version;
		return acc;
	}, {
		install: [],
		version
	});
	assert(result.install.length > 0, `Version ${version} not found.`);
	return flatten(schema, result.install, result.version);
}
function getAll(schema) {
	return [{
		release: "11.1.0",
		version: 26,
		previous: 25,
		install: [`
        CREATE OR REPLACE FUNCTION ${schema}.create_queue(queue_name text, options jsonb)
        RETURNS VOID AS
        $$
        DECLARE
          tablename varchar := CASE WHEN options->>'partition' = 'true'
                                THEN 'j' || encode(sha224(queue_name::bytea), 'hex')
                                ELSE 'job_common'
                                END;
          queue_created_on timestamptz;
        BEGIN

          WITH q as (
            INSERT INTO ${schema}.queue (
              name,
              policy,
              retry_limit,
              retry_delay,
              retry_backoff,
              retry_delay_max,
              expire_seconds,
              retention_seconds,
              deletion_seconds,
              warning_queued,
              dead_letter,
              partition,
              table_name
            )
            VALUES (
              queue_name,
              options->>'policy',
              COALESCE((options->>'retryLimit')::int, 2),
              COALESCE((options->>'retryDelay')::int, 0),
              COALESCE((options->>'retryBackoff')::bool, false),
              (options->>'retryDelayMax')::int,
              COALESCE((options->>'expireInSeconds')::int, 900),
              COALESCE((options->>'retentionSeconds')::int, 1209600),
              COALESCE((options->>'deleteAfterSeconds')::int, 604800),
              COALESCE((options->>'warningQueueSize')::int, 0),
              options->>'deadLetter',
              COALESCE((options->>'partition')::bool, false),
              tablename
            )
            ON CONFLICT DO NOTHING
            RETURNING created_on
          )
          SELECT created_on into queue_created_on from q;

          IF queue_created_on IS NULL OR options->>'partition' IS DISTINCT FROM 'true' THEN
            RETURN;
          END IF;

          EXECUTE format('CREATE TABLE ${schema}.%I (LIKE ${schema}.job INCLUDING DEFAULTS)', tablename);

          EXECUTE format('ALTER TABLE ${schema}.%1$I ADD PRIMARY KEY (name, id)', tablename);
          EXECUTE format('ALTER TABLE ${schema}.%1$I ADD CONSTRAINT q_fkey FOREIGN KEY (name) REFERENCES ${schema}.queue (name) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED', tablename);
          EXECUTE format('ALTER TABLE ${schema}.%1$I ADD CONSTRAINT dlq_fkey FOREIGN KEY (dead_letter) REFERENCES ${schema}.queue (name) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED', tablename);

          EXECUTE format('CREATE INDEX %1$s_i5 ON ${schema}.%1$I (name, start_after) INCLUDE (priority, created_on, id) WHERE state < ''active''', tablename);
          EXECUTE format('CREATE UNIQUE INDEX %1$s_i4 ON ${schema}.%1$I (name, singleton_on, COALESCE(singleton_key, '''')) WHERE state <> ''cancelled'' AND singleton_on IS NOT NULL', tablename);

          IF options->>'policy' = 'short' THEN
            EXECUTE format('CREATE UNIQUE INDEX %1$s_i1 ON ${schema}.%1$I (name, COALESCE(singleton_key, '''')) WHERE state = ''created'' AND policy = ''short''', tablename);
          ELSIF options->>'policy' = 'singleton' THEN
            EXECUTE format('CREATE UNIQUE INDEX %1$s_i2 ON ${schema}.%1$I (name, COALESCE(singleton_key, '''')) WHERE state = ''active'' AND policy = ''singleton''', tablename);
          ELSIF options->>'policy' = 'stately' THEN
            EXECUTE format('CREATE UNIQUE INDEX %1$s_i3 ON ${schema}.%1$I (name, state, COALESCE(singleton_key, '''')) WHERE state <= ''active'' AND policy = ''stately''', tablename);
          ELSIF options->>'policy' = 'exclusive' THEN
            EXECUTE format('CREATE UNIQUE INDEX %1$s_i6 ON ${schema}.%1$I (name, COALESCE(singleton_key, '''')) WHERE state <= ''active'' AND policy = ''exclusive''', tablename);
          END IF;

          EXECUTE format('ALTER TABLE ${schema}.%I ADD CONSTRAINT cjc CHECK (name=%L)', tablename, queue_name);
          EXECUTE format('ALTER TABLE ${schema}.job ATTACH PARTITION ${schema}.%I FOR VALUES IN (%L)', tablename, queue_name);
        END;
        $$
        LANGUAGE plpgsql;
        `, `CREATE UNIQUE INDEX job_i6 ON ${schema}.job_common (name, COALESCE(singleton_key, '')) WHERE state <= 'active' AND policy = 'exclusive'`],
		uninstall: [`DROP INDEX ${schema}.job_i6`]
	}];
}

//#endregion
//#region package.json
var pgboss = { "schema": 26 };

//#endregion
//#region src/contractor.ts
const schemaVersion = pgboss.schema;
var Contractor = class {
	static constructionPlans(schema = DEFAULT_SCHEMA, options = { createSchema: true }) {
		return create(schema, schemaVersion, options);
	}
	static migrationPlans(schema = DEFAULT_SCHEMA, version = schemaVersion - 1) {
		return migrate(schema, version);
	}
	static rollbackPlans(schema = DEFAULT_SCHEMA, version = schemaVersion) {
		return rollback(schema, version);
	}
	config;
	db;
	migrations;
	constructor(db, config) {
		this.config = config;
		this.db = db;
		this.migrations = this.config.migrations || getAll(this.config.schema);
	}
	async schemaVersion() {
		const result = await this.db.executeSql(getVersion(this.config.schema));
		return result.rows.length ? parseInt(result.rows[0].version) : null;
	}
	async isInstalled() {
		return !!(await this.db.executeSql(versionTableExists(this.config.schema))).rows[0].name;
	}
	async start() {
		if (await this.isInstalled()) {
			const version = await this.schemaVersion();
			if (version !== null && schemaVersion > version) await this.migrate(version);
		} else await this.create();
	}
	async check() {
		if (!await this.isInstalled()) throw new Error("pg-boss is not installed");
		if (schemaVersion !== await this.schemaVersion()) throw new Error("pg-boss database requires migrations");
	}
	async create() {
		try {
			const commands = create(this.config.schema, schemaVersion, this.config);
			await this.db.executeSql(commands);
		} catch (err) {
			assert(err.message.includes(CREATE_RACE_MESSAGE), err);
		}
	}
	async migrate(version) {
		try {
			const commands = migrate(this.config.schema, version, this.migrations);
			await this.db.executeSql(commands);
		} catch (err) {
			assert(err.message.includes(MIGRATE_RACE_MESSAGE), err);
		}
	}
	async next(version) {
		const commands = next(this.config.schema, version, this.migrations);
		await this.db.executeSql(commands);
	}
	async rollback(version) {
		const commands = rollback(this.config.schema, version, this.migrations);
		await this.db.executeSql(commands);
	}
};
var contractor_default = Contractor;

//#endregion
//#region src/timekeeper.ts
const QUEUES = { SEND_IT: "__pgboss__send-it" };
const EVENTS = {
	error: "error",
	schedule: "schedule",
	warning: "warning"
};
const WARNINGS$1 = { CLOCK_SKEW: { message: "Warning: Clock skew between this instance and the database server. This will not break scheduling, but is emitted any time the skew exceeds 60 seconds." } };
var Timekeeper = class extends EventEmitter {
	db;
	config;
	manager;
	stopped = true;
	cronMonitorInterval;
	skewMonitorInterval;
	timekeeping;
	clockSkew = 0;
	events = EVENTS;
	constructor(db, manager, config) {
		super();
		this.db = db;
		this.config = config;
		this.manager = manager;
	}
	async start() {
		this.stopped = false;
		await this.cacheClockSkew();
		await this.manager.createQueue(QUEUES.SEND_IT);
		const options = {
			pollingIntervalSeconds: this.config.cronWorkerIntervalSeconds,
			batchSize: 50
		};
		await this.manager.work(QUEUES.SEND_IT, options, (jobs) => this.onSendIt(jobs));
		setImmediate(() => this.onCron());
		this.cronMonitorInterval = setInterval(async () => await this.onCron(), this.config.cronMonitorIntervalSeconds * 1e3);
		this.skewMonitorInterval = setInterval(async () => await this.cacheClockSkew(), this.config.clockMonitorIntervalSeconds * 1e3);
	}
	async stop() {
		if (this.stopped) return;
		this.stopped = true;
		await this.manager.offWork(QUEUES.SEND_IT);
		if (this.skewMonitorInterval) {
			clearInterval(this.skewMonitorInterval);
			this.skewMonitorInterval = null;
		}
		if (this.cronMonitorInterval) {
			clearInterval(this.cronMonitorInterval);
			this.cronMonitorInterval = null;
		}
	}
	async cacheClockSkew() {
		let skew = 0;
		try {
			if (this.config.__test__force_clock_monitoring_error) throw new Error(this.config.__test__force_clock_monitoring_error);
			const { rows } = await this.db.executeSql(getTime());
			const local = Date.now();
			skew = parseFloat(rows[0].time) - local;
			const skewSeconds = Math.abs(skew) / 1e3;
			if (skewSeconds >= 60 || this.config.__test__force_clock_skew_warning) this.emit(this.events.warning, {
				message: WARNINGS$1.CLOCK_SKEW.message,
				data: {
					seconds: skewSeconds,
					direction: skew > 0 ? "slower" : "faster"
				}
			});
		} catch (err) {
			this.emit(this.events.error, err);
		} finally {
			this.clockSkew = skew;
		}
	}
	async onCron() {
		try {
			if (this.stopped || this.timekeeping) return;
			if (this.config.__test__force_cron_monitoring_error) throw new Error(this.config.__test__force_cron_monitoring_error);
			this.timekeeping = true;
			const sql = trySetCronTime(this.config.schema, this.config.cronMonitorIntervalSeconds);
			if (!this.stopped) {
				const { rows } = await this.db.executeSql(sql);
				if (!this.stopped && rows.length === 1) await this.cron();
			}
		} catch (err) {
			this.emit(this.events.error, err);
		} finally {
			this.timekeeping = false;
		}
	}
	async cron() {
		const scheduled = (await this.getSchedules()).filter((i) => this.shouldSendIt(i.cron, i.timezone)).map(({ name, key, data, options }) => ({
			data: {
				name,
				data,
				options
			},
			singletonKey: `${name}__${key}`,
			singletonSeconds: 60
		}));
		if (scheduled.length > 0 && !this.stopped) await this.manager.insert(QUEUES.SEND_IT, scheduled);
	}
	shouldSendIt(cron, tz) {
		const prevTime = CronExpressionParser.parse(cron, {
			tz,
			strict: false
		}).prev();
		return (Date.now() + this.clockSkew - prevTime.getTime()) / 1e3 < 60;
	}
	async onSendIt(jobs) {
		await Promise.allSettled(jobs.map(({ data }) => this.manager.send(data)));
	}
	async getSchedules(name, key = "") {
		let sql = getSchedules(this.config.schema);
		let params = [];
		if (name) {
			sql = getSchedulesByQueue(this.config.schema);
			params = [name, key];
		}
		const { rows } = await this.db.executeSql(sql, params);
		return rows;
	}
	async schedule(name, cron, data, options = {}) {
		const { tz = "UTC", key = "",...rest } = options;
		CronExpressionParser.parse(cron, {
			tz,
			strict: false
		});
		checkSendArgs([
			name,
			data,
			{ ...rest }
		]);
		assertKey(key);
		try {
			const sql = schedule(this.config.schema);
			await this.db.executeSql(sql, [
				name,
				key,
				cron,
				tz,
				data,
				options
			]);
		} catch (err) {
			if (err.message.includes("foreign key")) err.message = `Queue ${name} not found`;
			throw err;
		}
	}
	async unschedule(name, key = "") {
		const sql = unschedule(this.config.schema);
		await this.db.executeSql(sql, [name, key]);
	}
};
var timekeeper_default = Timekeeper;

//#endregion
//#region src/tools.ts
/**
* When sql contains multiple queries, result is an array of objects with rows property
* This function unwraps the result into a single object with rows property
*/
function unwrapSQLResult(result) {
	if (Array.isArray(result)) return { rows: result.flatMap((i) => i.rows) };
	return result;
}
function delay(ms, error) {
	const ac = new AbortController();
	const promise = new Promise((resolve, reject) => {
		setTimeout(ms, null, { signal: ac.signal }).then(() => {
			if (error) reject(new Error(error));
			else resolve();
		}).catch(resolve);
	});
	promise.abort = () => {
		if (!ac.signal.aborted) ac.abort();
	};
	return promise;
}
async function resolveWithinSeconds(promise, seconds, message) {
	const reject = delay(Math.max(1, seconds) * 1e3, message);
	let result;
	try {
		result = await Promise.race([promise, reject]);
	} finally {
		reject.abort();
	}
	return result;
}

//#endregion
//#region src/worker.ts
const WORKER_STATES = {
	created: "created",
	active: "active",
	stopping: "stopping",
	stopped: "stopped"
};
var Worker = class {
	id;
	name;
	options;
	fetch;
	onFetch;
	onError;
	interval;
	jobs = [];
	createdOn = Date.now();
	state = WORKER_STATES.created;
	lastFetchedOn = null;
	lastJobStartedOn = null;
	lastJobEndedOn = null;
	lastJobDuration = null;
	lastError = null;
	lastErrorOn = null;
	stopping = false;
	stopped = false;
	loopDelayPromise = null;
	beenNotified = false;
	constructor({ id, name, options, interval, fetch, onFetch, onError }) {
		this.id = id;
		this.name = name;
		this.options = options;
		this.fetch = fetch;
		this.onFetch = onFetch;
		this.onError = onError;
		this.interval = interval;
	}
	notify() {
		this.beenNotified = true;
		if (this.loopDelayPromise) this.loopDelayPromise.abort();
	}
	async start() {
		this.state = WORKER_STATES.active;
		while (!this.stopping) {
			const started = Date.now();
			try {
				this.beenNotified = false;
				const jobs = await this.fetch();
				this.lastFetchedOn = Date.now();
				if (jobs) {
					this.jobs = jobs;
					this.lastJobStartedOn = this.lastFetchedOn;
					await this.onFetch(jobs);
					this.lastJobEndedOn = Date.now();
					this.jobs = [];
				}
			} catch (err) {
				this.lastErrorOn = Date.now();
				this.lastError = err;
				err.message = `${err.message} (Queue: ${this.name}, Worker: ${this.id})`;
				this.onError(err);
			}
			const duration = Date.now() - started;
			this.lastJobDuration = duration;
			if (!this.stopping && !this.beenNotified && this.interval - duration > 100) {
				this.loopDelayPromise = delay(this.interval - duration);
				await this.loopDelayPromise;
				this.loopDelayPromise = null;
			}
		}
		this.stopping = false;
		this.stopped = true;
		this.state = WORKER_STATES.stopped;
	}
	stop() {
		this.stopping = true;
		this.state = WORKER_STATES.stopping;
		if (this.loopDelayPromise) this.loopDelayPromise.abort();
	}
	toWipData() {
		return {
			id: this.id,
			name: this.name,
			options: this.options,
			state: this.state,
			count: this.jobs.length,
			createdOn: this.createdOn,
			lastFetchedOn: this.lastFetchedOn,
			lastJobStartedOn: this.lastJobStartedOn,
			lastJobEndedOn: this.lastJobEndedOn,
			lastError: this.lastError,
			lastErrorOn: this.lastErrorOn,
			lastJobDuration: this.lastJobDuration
		};
	}
};
var worker_default = Worker;

//#endregion
//#region src/manager.ts
const INTERNAL_QUEUES = Object.values(QUEUES).reduce((acc, i) => ({
	...acc,
	[i]: i
}), {});
const events$2 = {
	error: "error",
	wip: "wip"
};
var Manager = class extends EventEmitter {
	events = events$2;
	db;
	config;
	wipTs;
	workers;
	stopped;
	queueCacheInterval;
	timekeeper;
	queues;
	constructor(db, config) {
		super();
		this.config = config;
		this.db = db;
		this.wipTs = Date.now();
		this.workers = /* @__PURE__ */ new Map();
		this.queues = null;
	}
	async start() {
		this.stopped = false;
		this.queueCacheInterval = setInterval(() => this.onCacheQueues({ emit: true }), this.config.queueCacheIntervalSeconds * 1e3);
		await this.onCacheQueues();
	}
	async onCacheQueues({ emit = false } = {}) {
		try {
			assert(!this.config.__test__throw_queueCache, "test error");
			this.queues = (await this.getQueues()).reduce((acc, i) => {
				acc[i.name] = i;
				return acc;
			}, {});
		} catch (error) {
			emit && this.emit(events$2.error, {
				...error,
				message: error.message,
				stack: error.stack
			});
		}
	}
	async getQueueCache(name) {
		assert(this.queues, "Queue cache is not initialized");
		let queue = this.queues[name];
		if (queue) return queue;
		queue = await this.getQueue(name);
		if (!queue) throw new Error(`Queue ${name} does not exist`);
		this.queues[name] = queue;
		return queue;
	}
	async stop() {
		this.stopped = true;
		clearInterval(this.queueCacheInterval);
		for (const worker of this.workers.values()) if (!INTERNAL_QUEUES[worker.name]) await this.offWork(worker.name);
	}
	async failWip() {
		for (const worker of this.workers.values()) {
			const jobIds = worker.jobs.map((j) => j.id);
			if (jobIds.length) await this.fail(worker.name, jobIds, "pg-boss shut down while active");
		}
	}
	async work(name, ...args) {
		const { options, callback } = checkWorkArgs(name, args);
		return await this.watch(name, options, callback);
	}
	addWorker(worker) {
		this.workers.set(worker.id, worker);
	}
	removeWorker(worker) {
		this.workers.delete(worker.id);
	}
	getWorkers() {
		return Array.from(this.workers.values());
	}
	emitWip(name) {
		if (!INTERNAL_QUEUES[name]) {
			const now = Date.now();
			if (now - this.wipTs > 2e3) {
				this.emit(events$2.wip, this.getWipData());
				this.wipTs = now;
			}
		}
	}
	getWipData(options = {}) {
		const { includeInternal = false } = options;
		return this.getWorkers().map((i) => i.toWipData()).filter((i) => i.count > 0 && (!INTERNAL_QUEUES[i.name] || includeInternal));
	}
	async watch(name, options, callback) {
		if (this.stopped) throw new Error("Workers are disabled. pg-boss is stopped");
		const { pollingInterval: interval, batchSize, includeMetadata = false, priority = true } = options;
		const id = randomUUID({ disableEntropyCache: true });
		const fetch = () => this.fetch(name, {
			batchSize,
			includeMetadata,
			priority
		});
		const onFetch = async (jobs) => {
			if (!jobs.length) return;
			if (this.config.__test__throw_worker) throw new Error("__test__throw_worker");
			this.emitWip(name);
			const maxExpiration = jobs.reduce((acc, i) => Math.max(acc, i.expireInSeconds), 0);
			const jobIds = jobs.map((job) => job.id);
			try {
				const result = await resolveWithinSeconds(callback(jobs), maxExpiration, `handler execution exceeded ${maxExpiration}s`);
				await this.complete(name, jobIds, jobIds.length === 1 ? result : void 0);
			} catch (err) {
				await this.fail(name, jobIds, err);
			}
			this.emitWip(name);
		};
		const onError = (error) => {
			this.emit(events$2.error, {
				...error,
				message: error.message,
				stack: error.stack,
				queue: name,
				worker: id
			});
		};
		const worker = new worker_default({
			id,
			name,
			options,
			interval,
			fetch,
			onFetch,
			onError
		});
		this.addWorker(worker);
		worker.start();
		return id;
	}
	async offWork(value) {
		assert(value, "Missing required argument");
		const query = typeof value === "string" ? { filter: (i) => i.name === value } : typeof value === "object" && value.id ? { filter: (i) => i.id === value.id } : null;
		assert(query, "Invalid argument. Expected string or object: { id }");
		const workers = this.getWorkers().filter((i) => query.filter(i) && !i.stopping && !i.stopped);
		if (workers.length === 0) return;
		for (const worker of workers) worker.stop();
		setImmediate(async () => {
			let timeout = 6000; // this is a 6 second hardcoded timeout
			let startTime = Date.now();

			console.log('Started offWork')
		
			while (!workers.every((w) => w.stopped) && (Date.now() - startTime) < timeout) {
				await delay(1e3);
				console.log('Time elapsed:', Date.now() - startTime, 'ms')
				// console.log('Workers stopped:', workers.every((w) => w.stopped))
			}
		
			// console.log({ workers })
		
			if (timeout && (Date.now() - startTime) >= timeout) {
				console.log('Workers did not stop in time, forcing removal after', timeout, 'ms')
			}
		
			for (const worker of workers) this.removeWorker(worker);
		});
	}
	notifyWorker(workerId) {
		this.workers.get(workerId)?.notify();
	}
	async subscribe(event, name) {
		assert(event, "Missing required argument");
		assert(name, "Missing required argument");
		const sql = subscribe(this.config.schema);
		await this.db.executeSql(sql, [event, name]);
	}
	async unsubscribe(event, name) {
		assert(event, "Missing required argument");
		assert(name, "Missing required argument");
		const sql = unsubscribe(this.config.schema);
		await this.db.executeSql(sql, [event, name]);
	}
	async publish(event, data, options) {
		assert(event, "Missing required argument");
		const sql = getQueuesForEvent(this.config.schema);
		const { rows } = await this.db.executeSql(sql, [event]);
		await Promise.allSettled(rows.map(({ name }) => this.send(name, data, options)));
	}
	async send(...args) {
		const result = checkSendArgs(args);
		return await this.createJob(result);
	}
	async sendAfter(name, data, options, after) {
		options = options ? { ...options } : {};
		options.startAfter = after;
		const result = checkSendArgs([
			name,
			data,
			options
		]);
		return await this.createJob(result);
	}
	async sendThrottled(name, data, options, seconds, key) {
		options = options ? { ...options } : {};
		options.singletonSeconds = seconds;
		options.singletonNextSlot = false;
		options.singletonKey = key;
		const result = checkSendArgs([
			name,
			data,
			options
		]);
		return await this.createJob(result);
	}
	async sendDebounced(name, data, options, seconds, key) {
		options = options ? { ...options } : {};
		options.singletonSeconds = seconds;
		options.singletonNextSlot = true;
		options.singletonKey = key;
		const result = checkSendArgs([
			name,
			data,
			options
		]);
		return await this.createJob(result);
	}
	async createJob(request) {
		const { name, data = null, options = {} } = request;
		const { id = null, db: wrapper, priority, startAfter, singletonKey = null, singletonSeconds, singletonNextSlot, expireInSeconds, deleteAfterSeconds, retentionSeconds, keepUntil, retryLimit, retryDelay, retryBackoff, retryDelayMax } = options;
		const job = {
			id,
			name,
			data,
			priority,
			startAfter,
			singletonKey,
			singletonSeconds,
			singletonOffset: 0,
			expireInSeconds,
			deleteAfterSeconds,
			retentionSeconds,
			keepUntil,
			retryLimit,
			retryDelay,
			retryBackoff,
			retryDelayMax
		};
		const db = wrapper || this.db;
		const { table } = await this.getQueueCache(name);
		const sql = insertJobs(this.config.schema, {
			table,
			name,
			returnId: true
		});
		const { rows: try1 } = await db.executeSql(sql, [JSON.stringify([job])]);
		if (try1.length === 1) return try1[0].id;
		if (singletonNextSlot) {
			job.startAfter = this.getDebounceStartAfter(singletonSeconds, this.timekeeper.clockSkew);
			job.singletonOffset = singletonSeconds;
			const { rows: try2 } = await db.executeSql(sql, [JSON.stringify([job])]);
			if (try2.length === 1) return try2[0].id;
		}
		return null;
	}
	async insert(name, jobs, options = {}) {
		assert(Array.isArray(jobs), "jobs argument should be an array");
		const { table } = await this.getQueueCache(name);
		const db = this.assertDb(options);
		const sql = insertJobs(this.config.schema, {
			table,
			name,
			returnId: false
		});
		const { rows } = await db.executeSql(sql, [JSON.stringify(jobs)]);
		return rows.length ? rows.map((i) => i.id) : null;
	}
	getDebounceStartAfter(singletonSeconds, clockOffset) {
		const debounceInterval = singletonSeconds * 1e3;
		const now = Date.now() + clockOffset;
		const slot = Math.floor(now / debounceInterval) * debounceInterval;
		let startAfter = singletonSeconds - Math.floor((now - slot) / 1e3) || 1;
		if (singletonSeconds > 1) startAfter++;
		return startAfter;
	}
	async fetch(name, options = {}) {
		checkFetchArgs(name, options);
		const db = this.assertDb(options);
		const { table, policy, singletonsActive } = await this.getQueueCache(name);
		const fetchOptions = {
			...options,
			schema: this.config.schema,
			table,
			name,
			policy,
			limit: options.batchSize,
			ignoreSingletons: singletonsActive
		};
		const sql = fetchNextJob(fetchOptions);
		let result;
		try {
			result = await db.executeSql(sql);
		} catch (err) {}
		return result?.rows || [];
	}
	mapCompletionIdArg(id, funcName) {
		const errorMessage = `${funcName}() requires an id`;
		assert(id, errorMessage);
		const ids = Array.isArray(id) ? id : [id];
		assert(ids.length, errorMessage);
		return ids;
	}
	mapCompletionDataArg(data) {
		if (data === null || typeof data === "undefined" || typeof data === "function") return null;
		return serializeError(typeof data === "object" && !Array.isArray(data) ? data : { value: data });
	}
	mapCommandResponse(ids, result) {
		return {
			jobs: ids,
			requested: ids.length,
			affected: result && result.rows ? parseInt(result.rows[0].count) : 0
		};
	}
	async complete(name, id, data, options = {}) {
		assertQueueName(name);
		const db = this.assertDb(options);
		const ids = this.mapCompletionIdArg(id, "complete");
		const { table } = await this.getQueueCache(name);
		const sql = completeJobs(this.config.schema, table);
		const result = await db.executeSql(sql, [
			name,
			ids,
			this.mapCompletionDataArg(data)
		]);
		return this.mapCommandResponse(ids, result);
	}
	async fail(name, id, data, options = {}) {
		assertQueueName(name);
		const db = this.assertDb(options);
		const ids = this.mapCompletionIdArg(id, "fail");
		const { table } = await this.getQueueCache(name);
		const sql = failJobsById(this.config.schema, table);
		const result = await db.executeSql(sql, [
			name,
			ids,
			this.mapCompletionDataArg(data)
		]);
		return this.mapCommandResponse(ids, result);
	}
	async cancel(name, id, options = {}) {
		assertQueueName(name);
		const db = this.assertDb(options);
		const ids = this.mapCompletionIdArg(id, "cancel");
		const { table } = await this.getQueueCache(name);
		const sql = cancelJobs(this.config.schema, table);
		const result = await db.executeSql(sql, [name, ids]);
		return this.mapCommandResponse(ids, result);
	}
	async deleteJob(name, id, options = {}) {
		assertQueueName(name);
		const db = this.assertDb(options);
		const ids = this.mapCompletionIdArg(id, "deleteJob");
		const { table } = await this.getQueueCache(name);
		const sql = deleteJobsById(this.config.schema, table);
		const result = await db.executeSql(sql, [name, ids]);
		return this.mapCommandResponse(ids, result);
	}
	async resume(name, id, options = {}) {
		assertQueueName(name);
		const db = this.assertDb(options);
		const ids = this.mapCompletionIdArg(id, "resume");
		const { table } = await this.getQueueCache(name);
		const sql = resumeJobs(this.config.schema, table);
		const result = await db.executeSql(sql, [name, ids]);
		return this.mapCommandResponse(ids, result);
	}
	async retry(name, id, options = {}) {
		assertQueueName(name);
		const db = options.db || this.db;
		const ids = this.mapCompletionIdArg(id, "retry");
		const { table } = await this.getQueueCache(name);
		const sql = retryJobs(this.config.schema, table);
		const result = await db.executeSql(sql, [name, ids]);
		return this.mapCommandResponse(ids, result);
	}
	async createQueue(name, options = {}) {
		name = name || options.name;
		assertQueueName(name);
		options.policy = options.policy || QUEUE_POLICIES.standard;
		assert(options.policy in QUEUE_POLICIES, `${options.policy} is not a valid queue policy`);
		validateQueueArgs(options);
		if (options.deadLetter) {
			assertQueueName(options.deadLetter);
			notStrictEqual(name, options.deadLetter, "deadLetter cannot be itself");
			await this.getQueueCache(options.deadLetter);
		}
		const sql = createQueue(this.config.schema, name, options);
		await this.db.executeSql(sql);
	}
	async getQueues(names) {
		names = Array.isArray(names) ? names : typeof names === "string" ? [names] : void 0;
		if (names) for (const name of names) assertQueueName(name);
		const sql = getQueues(this.config.schema, names);
		const { rows } = await this.db.executeSql(sql);
		return rows;
	}
	async updateQueue(name, options = {}) {
		assertQueueName(name);
		assert(Object.keys(options).length > 0, "no properties found to update");
		if ("policy" in options) throw new Error("queue policy cannot be changed after creation");
		if ("partition" in options) throw new Error("queue partitioning cannot be changed after creation");
		validateQueueArgs(options);
		const { deadLetter } = options;
		if (deadLetter) {
			assertQueueName(deadLetter);
			notStrictEqual(name, deadLetter, "deadLetter cannot be itself");
		}
		const sql = updateQueue(this.config.schema, { deadLetter });
		await this.db.executeSql(sql, [name, options]);
	}
	async getQueue(name) {
		assertQueueName(name);
		const sql = getQueues(this.config.schema, [name]);
		const { rows } = await this.db.executeSql(sql);
		return rows[0] || null;
	}
	async deleteQueue(name) {
		assertQueueName(name);
		try {
			await this.getQueueCache(name);
			const sql = deleteQueue(this.config.schema, name);
			await this.db.executeSql(sql);
		} catch {}
	}
	async deleteQueuedJobs(name) {
		assertQueueName(name);
		const { table } = await this.getQueueCache(name);
		const sql = deleteQueuedJobs(this.config.schema, table);
		await this.db.executeSql(sql, [name]);
	}
	async deleteStoredJobs(name) {
		assertQueueName(name);
		const { table } = await this.getQueueCache(name);
		const sql = deleteStoredJobs(this.config.schema, table);
		await this.db.executeSql(sql, [name]);
	}
	async deleteAllJobs(name) {
		assertQueueName(name);
		const { table, partition } = await this.getQueueCache(name);
		if (partition) {
			const sql = truncateTable(this.config.schema, table);
			await this.db.executeSql(sql);
		} else {
			const sql = deleteAllJobs(this.config.schema, table);
			await this.db.executeSql(sql, [name]);
		}
	}
	async getQueueStats(name) {
		assertQueueName(name);
		const queue = await this.getQueueCache(name);
		const sql = getQueueStats(this.config.schema, queue.table, [name]);
		const { rows } = await this.db.executeSql(sql);
		return Object.assign(queue, rows.at(0) || {});
	}
	async getJobById(name, id, options = {}) {
		assertQueueName(name);
		const db = this.assertDb(options);
		const { table } = await this.getQueueCache(name);
		const sql = getJobById(this.config.schema, table);
		const result1 = await db.executeSql(sql, [name, id]);
		if (result1?.rows?.length === 1) return result1.rows[0];
		else return null;
	}
	assertDb(options) {
		if (options.db) return options.db;
		if (this.db._pgbdb) assert(this.db.opened, "Database connection is not opened");
		return this.db;
	}
};
var manager_default = Manager;

//#endregion
//#region src/boss.ts
const events$1 = {
	error: "error",
	warning: "warning"
};
const WARNINGS = {
	SLOW_QUERY: {
		seconds: 30,
		message: "Warning: slow query. Your queues and/or database server should be reviewed"
	},
	LARGE_QUEUE: {
		size: 1e4,
		message: "Warning: large queue backlog. Your queue should be reviewed"
	}
};
var Boss = class extends EventEmitter {
	#stopped;
	#maintaining;
	#superviseInterval;
	#db;
	#config;
	#manager;
	events = events$1;
	constructor(db, manager, config) {
		super();
		this.#db = db;
		this.#config = config;
		this.#manager = manager;
		this.#stopped = true;
		if (config.warningSlowQuerySeconds) WARNINGS.SLOW_QUERY.seconds = config.warningSlowQuerySeconds;
		if (config.warningQueueSize) WARNINGS.LARGE_QUEUE.size = config.warningQueueSize;
	}
	async start() {
		if (this.#stopped) {
			this.#superviseInterval = setInterval(() => this.#onSupervise(), this.#config.superviseIntervalSeconds * 1e3);
			this.#stopped = false;
		}
	}
	async stop() {
		if (!this.#stopped) {
			if (this.#superviseInterval) clearInterval(this.#superviseInterval);
			this.#stopped = true;
		}
	}
	async #executeSql(sql, values) {
		const started = Date.now();
		const result = unwrapSQLResult(await this.#db.executeSql(sql, values));
		const elapsed = (Date.now() - started) / 1e3;
		if (elapsed > WARNINGS.SLOW_QUERY.seconds || this.#config.__test__warn_slow_query) this.emit(events$1.warning, {
			message: WARNINGS.SLOW_QUERY.message,
			data: {
				elapsed,
				sql,
				values
			}
		});
		return result;
	}
	async #onSupervise() {
		try {
			if (this.#stopped) return;
			if (this.#maintaining) return;
			if (this.#config.__test__throw_maint) throw new Error(this.#config.__test__throw_maint);
			this.#maintaining = true;
			const queues = await this.#manager.getQueues();
			for (const queue of queues) !this.#stopped && await this.supervise(queue);
		} catch (err) {
			this.emit(events$1.error, err);
		} finally {
			this.#maintaining = false;
		}
	}
	async supervise(value) {
		let queues;
		if (typeof value === "object") queues = [value];
		else queues = await this.#manager.getQueues(value);
		const queueGroups = queues.reduce((acc, q) => {
			const { table } = q;
			acc[table] = acc[table] || {
				table,
				queues: []
			};
			acc[table].queues.push(q);
			return acc;
		}, {});
		for (const queueGroup of Object.values(queueGroups)) {
			const { table, queues: queues$1 } = queueGroup;
			const names = queues$1.map((i) => i.name);
			while (names.length) {
				const chunk = names.splice(0, 100);
				await this.#monitor(table, chunk);
				await this.#maintain(table, chunk);
			}
		}
	}
	async #monitor(table, names) {
		const command = trySetQueueMonitorTime(this.#config.schema, names, this.#config.monitorIntervalSeconds);
		const { rows } = await this.#executeSql(command);
		if (rows.length) {
			const queues = rows.map((q) => q.name);
			const cacheStatsSql = cacheQueueStats(this.#config.schema, table, queues);
			const { rows: rowsCacheStats } = await this.#executeSql(cacheStatsSql);
			const warnings = rowsCacheStats.filter((i) => i.queuedCount > (i.warningQueueSize || WARNINGS.LARGE_QUEUE.size));
			for (const warning of warnings) this.emit(events$1.warning, {
				message: WARNINGS.LARGE_QUEUE.message,
				data: warning
			});
			const sql = failJobsByTimeout(this.#config.schema, table, queues);
			await this.#executeSql(sql);
		}
	}
	async #maintain(table, names) {
		const command = trySetQueueDeletionTime(this.#config.schema, names, this.#config.maintenanceIntervalSeconds);
		const { rows } = await this.#executeSql(command);
		if (rows.length) {
			const queues = rows.map((q) => q.name);
			const sql = deletion(this.#config.schema, table, queues);
			await this.#executeSql(sql);
		}
	}
};
var boss_default = Boss;

//#endregion
//#region src/db.ts
var Db = class extends EventEmitter {
	pool;
	config;
	/** @internal */
	_pgbdb;
	opened;
	constructor(config) {
		super();
		config.application_name = config.application_name || "pgboss";
		this.config = config;
		this._pgbdb = true;
		this.opened = false;
	}
	events = { error: "error" };
	async open() {
		this.pool = new pg.Pool(this.config);
		this.pool.on("error", (error) => this.emit("error", error));
		this.opened = true;
	}
	async close() {
		if (!this.pool.ending) {
			this.opened = false;
			await this.pool.end();
		}
	}
	async executeSql(text, values) {
		assert(this.opened, "Database not opened. Call open() before executing SQL.");
		return await this.pool.query(text, values);
	}
};
var db_default = Db;

//#endregion
//#region src/index.ts
const events = Object.freeze({
	error: "error",
	warning: "warning",
	wip: "wip",
	stopped: "stopped"
});
function getConstructionPlans(schema) {
	return contractor_default.constructionPlans(schema);
}
function getMigrationPlans(schema, version) {
	return contractor_default.migrationPlans(schema, version);
}
function getRollbackPlans(schema, version) {
	return contractor_default.rollbackPlans(schema, version);
}
var PgBoss = class extends EventEmitter {
	#stoppingOn;
	#stopped;
	#starting;
	#started;
	#config;
	#db;
	#boss;
	#contractor;
	#manager;
	#timekeeper;
	constructor(value) {
		super();
		this.#stoppingOn = null;
		this.#stopped = true;
		const config = getConfig(value);
		this.#config = config;
		const db = this.getDb();
		this.#db = db;
		if ("_pgbdb" in this.#db && this.#db._pgbdb) this.#promoteEvents(this.#db);
		const contractor = new contractor_default(db, config);
		const manager = new manager_default(db, config);
		const boss = new boss_default(db, manager, config);
		const timekeeper = new timekeeper_default(db, manager, config);
		manager.timekeeper = timekeeper;
		this.#promoteEvents(manager);
		this.#promoteEvents(boss);
		this.#promoteEvents(timekeeper);
		this.#boss = boss;
		this.#contractor = contractor;
		this.#manager = manager;
		this.#timekeeper = timekeeper;
	}
	#promoteEvents(emitter) {
		for (const event of Object.values(emitter?.events)) emitter.on(event, (arg) => this.emit(event, arg));
	}
	async start() {
		if (this.#starting || this.#started) return this;
		this.#starting = true;
		if (this.#db._pgbdb && !this.#db.opened) await this.#db.open();
		if (this.#config.migrate) await this.#contractor.start();
		else await this.#contractor.check();
		await this.#manager.start();
		if (this.#config.supervise) await this.#boss.start();
		if (this.#config.schedule) await this.#timekeeper.start();
		this.#starting = false;
		this.#started = true;
		this.#stopped = false;
		return this;
	}
	async stop(options = {}) {
		if (this.#stoppingOn || this.#stopped) return;
		let { close = true, graceful = true, timeout = 3e4 } = options;
		timeout = Math.max(timeout, 1e3);
		this.#stoppingOn = Date.now();
		await this.#manager.stop();
		await this.#timekeeper.stop();
		await this.#boss.stop();
		const shutdown = async () => {
			await this.#manager.failWip();
			if (this.#db._pgbdb && this.#db.opened && close) await this.#db.close();
			this.#stopped = true;
			this.#stoppingOn = null;
			this.#started = false;
			this.emit(events.stopped);
		};
		if (!graceful) return await shutdown();
		const isWip = () => this.#manager.getWipData({ includeInternal: false }).length > 0;
		while (Date.now() - this.#stoppingOn < timeout && isWip()) await delay(500);
		await shutdown();
	}
	async send(...args) {
		return await this.#manager.send(...args);
	}
	async sendAfter(name, data, options, after) {
		return this.#manager.sendAfter(name, data, options, after);
	}
	sendThrottled(name, data, options, seconds, key) {
		return this.#manager.sendThrottled(name, data, options, seconds, key);
	}
	sendDebounced(name, data, options, seconds, key) {
		return this.#manager.sendDebounced(name, data, options, seconds, key);
	}
	insert(name, jobs, options) {
		return this.#manager.insert(name, jobs, options);
	}
	fetch(name, options = {}) {
		return this.#manager.fetch(name, options);
	}
	work(...args) {
		return this.#manager.work(...args);
	}
	offWork(value) {
		return this.#manager.offWork(value);
	}
	notifyWorker(workerId) {
		this.#manager.notifyWorker(workerId);
	}
	subscribe(event, name) {
		return this.#manager.subscribe(event, name);
	}
	unsubscribe(event, name) {
		return this.#manager.unsubscribe(event, name);
	}
	publish(event, data, options) {
		return this.#manager.publish(event, data, options);
	}
	cancel(name, id, options) {
		return this.#manager.cancel(name, id, options);
	}
	resume(name, id, options) {
		return this.#manager.resume(name, id, options);
	}
	retry(name, id, options) {
		return this.#manager.retry(name, id, options);
	}
	deleteJob(name, id, options) {
		return this.#manager.deleteJob(name, id, options);
	}
	deleteQueuedJobs(name) {
		return this.#manager.deleteQueuedJobs(name);
	}
	deleteStoredJobs(name) {
		return this.#manager.deleteStoredJobs(name);
	}
	deleteAllJobs(name) {
		return this.#manager.deleteAllJobs(name);
	}
	complete(name, id, data, options) {
		return this.#manager.complete(name, id, data, options);
	}
	fail(name, id, data, options) {
		return this.#manager.fail(name, id, data, options);
	}
	getJobById(name, id, options) {
		return this.#manager.getJobById(name, id, options);
	}
	createQueue(name, options) {
		return this.#manager.createQueue(name, options);
	}
	updateQueue(name, options) {
		return this.#manager.updateQueue(name, options);
	}
	deleteQueue(name) {
		return this.#manager.deleteQueue(name);
	}
	getQueues(names) {
		return this.#manager.getQueues();
	}
	getQueue(name) {
		return this.#manager.getQueue(name);
	}
	getQueueStats(name) {
		return this.#manager.getQueueStats(name);
	}
	supervise(name) {
		return this.#boss.supervise(name);
	}
	isInstalled() {
		return this.#contractor.isInstalled();
	}
	schemaVersion() {
		return this.#contractor.schemaVersion();
	}
	schedule(name, cron, data, options) {
		return this.#timekeeper.schedule(name, cron, data, options);
	}
	unschedule(name, key) {
		return this.#timekeeper.unschedule(name, key);
	}
	getSchedules(name, key) {
		return this.#timekeeper.getSchedules(name, key);
	}
	getDb() {
		if (this.#db) return this.#db;
		if (this.#config.db) return this.#config.db;
		return new db_default(this.#config);
	}
};

//#endregion
export { PgBoss, events, getConstructionPlans, getMigrationPlans, getRollbackPlans, QUEUE_POLICIES as policies, JOB_STATES as states };
//# sourceMappingURL=index.mjs.map