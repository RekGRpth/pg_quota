#include "include.h"

#if PG_VERSION_NUM < 130000
#include <catalog/pg_type.h>
#include <miscadmin.h>
#endif
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>

PG_MODULE_MAGIC;

static int launcher_fetch;
static int launcher_restart;

static void pg_quota_init(bool dynamic) {
    BackgroundWorker worker = {0};
    size_t len;
    elog(DEBUG1, "dynamic = %s", dynamic ? "true" : "false");
    if ((len = strlcpy(worker.bgw_function_name, "pg_quota_launcher", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_quota", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = strlcpy(worker.bgw_name, "postgres pg_quota launcher", sizeof(worker.bgw_name))) >= sizeof(worker.bgw_name)) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_name))));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = launcher_restart;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (dynamic) {
        worker.bgw_notify_pid = MyProcPid;
        IsUnderPostmaster = true;
        if (!RegisterDynamicBackgroundWorker(&worker, NULL)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
        IsUnderPostmaster = false;
    } else RegisterBackgroundWorker(&worker);
}

void _PG_init(void) {
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("This module can only be loaded via shared_preload_libraries")));
    DefineCustomIntVariable("pg_quota.launcher_fetch", "pg_quota launcher fetch", "Fetch launcher rows at once", &launcher_fetch, 10, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_quota.launcher_restart", "pg_quota launcher restart", "Restart launcher interval, seconds", &launcher_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
#ifdef GP_VERSION_NUM
    if (!IS_QUERY_DISPATCHER()) return;
#endif
    pg_quota_init(false);
}

void pg_quota_launcher(Datum arg) {
    Portal portal;
    StringInfoData src;
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionMy("postgres", NULL);
    set_config_option_my("application_name", "pg_quota", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    pgstat_report_appname("pg_quota");
    set_ps_display_my("main");
    process_session_preload_libraries();
    if (!DatumGetBool(DirectFunctionCall2(pg_try_advisory_lock_int4, Int32GetDatum(MyDatabaseId), Int32GetDatum(GetUserId())))) { elog(WARNING, "!pg_try_advisory_lock_int4(%i, %i)", MyDatabaseId, GetUserId()); return; }
    initStringInfo(&src);
    appendStringInfo(&src, SQL(
        WITH _ AS (
            WITH _ AS (
                SELECT "setdatabase", regexp_split_to_array(UNNEST("setconfig"), '=') AS "setconfig" FROM "pg_db_role_setting"
            ) SELECT "setdatabase", %s(array_agg("setconfig"[1]), array_agg("setconfig"[2])) AS "setconfig" FROM _ GROUP BY 1
        ) SELECT "setdatabase", "datname"::text AS "data", "rolname"::text AS "user", ("setconfig"->>'pg_quota.sleep')::bigint AS "sleep",
        FROM _ INNER JOIN "pg_database" ON "pg_database"."oid" = "setdatabase" INNER JOIN "pg_roles" ON "pg_roles"."oid" = "datdba"
        LEFT JOIN "pg_locks" ON "locktype" = 'userlock' AND "mode" = 'AccessExclusiveLock' AND "granted" AND "objsubid" = 2 AND "database" = "setdatabase" AND "classid" = "setdatabase" AND "objid" = "datdba"
        WHERE "pid" IS NULL
    ),
#if PG_VERSION_NUM >= 90500
        "jsonb_object"
#else
        "json_object"
#endif
    );
    SPI_connect_my(src.data);
    portal = SPI_cursor_open_with_args_my(src.data, 0, NULL, NULL, NULL, true);
    do {
        SPI_cursor_fetch(portal, true, launcher_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            set_ps_display_my("row");
            elog(DEBUG1, "row = %lu, oid = %i, sleep = %li", row, DatumGetObjectId(SPI_getbinval_my(val, tupdesc, "setdatabase", false, OIDOID)), DatumGetInt64(SPI_getbinval_my(val, tupdesc, "sleep", false, INT8OID)));
        }
    } while (SPI_processed);
    SPI_cursor_close_my(portal);
    SPI_finish_my();
    pfree(src.data);
    set_ps_display_my("idle");
    if (!DatumGetBool(DirectFunctionCall2(pg_advisory_unlock_int4, Int32GetDatum(MyDatabaseId), Int32GetDatum(GetUserId())))) elog(WARNING, "!pg_advisory_unlock_int4(%i, %i)", MyDatabaseId, GetUserId());
}
