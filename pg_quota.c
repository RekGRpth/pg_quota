#include "include.h"

#if PG_VERSION_NUM < 130000
#include <catalog/pg_type.h>
#include <miscadmin.h>
#endif
#include <pgstat.h>
#include <postmaster/bgworker.h>
#if PG_VERSION_NUM < 130000
#include <signal.h>
#endif
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>

PG_MODULE_MAGIC;

typedef struct Worker {
    char data[NAMEDATALEN];
    char user[NAMEDATALEN];
    int64 timeout;
    Oid oid;
} Worker;

static int launcher_fetch;
static int launcher_restart;
static int worker_restart;

#if PG_VERSION_NUM < 130000
static volatile sig_atomic_t ShutdownRequestPending = false;

static void
SignalHandlerForConfigReload(SIGNAL_ARGS)
{
	int			save_errno = errno;

	ConfigReloadPending = true;
	SetLatch(MyLatch);

	errno = save_errno;
}
#endif

static void pg_quota_launcher_start(bool dynamic) {
    BackgroundWorker worker = {0};
    size_t len;
    elog(LOG, "dynamic = %s", dynamic ? "true" : "false");
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

static void pg_quota_worker_start(Worker *w) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker = {0};
    pid_t pid;
    size_t len;
    set_ps_display_my("work");
    if ((len = strlcpy(worker.bgw_function_name, "pg_quota_worker", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_quota", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_quota worker", w->user, w->data)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = ObjectIdGetDatum(w->oid);
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = worker_restart;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: elog(LOG, "started"); break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    if (handle) pfree(handle);
}

static void pg_quota_reload(void) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
}

static void pg_quota_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) pg_quota_reload();
}

static void pg_quota_timeout(void) {
    elog(LOG, "ShutdownRequestPending = %s", ShutdownRequestPending ? "true" : "false");
}

#if PG_VERSION_NUM < 110000
/*
 * Connect background worker to a database using OIDs.
 */
void
BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid)
{
	BackgroundWorker *worker = MyBgworkerEntry;

	/* XXX is this the right errcode? */
	if (!(worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION))
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("database connection requirement not indicated during registration")));

	InitPostgres(NULL, dboid, NULL, NULL);

	/* it had better not gotten out of "init" mode yet */
	if (!IsInitProcessingMode())
		ereport(ERROR,
				(errmsg("invalid processing mode in background worker")));
	SetProcessingMode(NormalProcessing);
}
#endif

void _PG_init(void) {
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("This module can only be loaded via shared_preload_libraries")));
    DefineCustomIntVariable("pg_quota.launcher_fetch", "pg_quota launcher fetch", "Fetch launcher rows at once", &launcher_fetch, 10, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_quota.launcher_restart", "pg_quota launcher restart", "Restart launcher interval, seconds", &launcher_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_quota.worker_restart", "pg_quota worker restart", "Restart worker interval, seconds", &worker_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    pg_quota_launcher_start(false);
}

void pg_quota_launcher(Datum arg) {
    Portal portal;
    StringInfoData src;
#ifdef GP_VERSION_NUM
    Gp_role = GP_ROLE_UTILITY;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_UTILITY;
#endif
#endif
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionMy("postgres", NULL);
    set_config_option_my("application_name", "pg_quota launcher", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    pgstat_report_appname("pg_quota launcher");
    set_ps_display_my("main");
    process_session_preload_libraries();
    if (!DatumGetBool(DirectFunctionCall2(pg_try_advisory_lock_int4, Int32GetDatum(MyDatabaseId), Int32GetDatum(GetUserId())))) { elog(WARNING, "!pg_try_advisory_lock_int4(%i, %i)", MyDatabaseId, GetUserId()); return; }
    initStringInfo(&src);
    appendStringInfo(&src, SQL(
        WITH _ AS (
            WITH _ AS (
                SELECT "setdatabase", regexp_split_to_array(UNNEST("setconfig"), '=') AS "setconfig" FROM "pg_db_role_setting"
            ) SELECT "setdatabase", %s(array_agg("setconfig"[1]), array_agg("setconfig"[2])) AS "setconfig" FROM _ GROUP BY 1
        ) SELECT "setdatabase", "datname"::text AS "data", "rolname"::text AS "user", ("setconfig"->>'pg_quota.timeout')::bigint AS "timeout"
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
            Worker w = {0};
            set_ps_display_my("row");
            w.oid = DatumGetObjectId(SPI_getbinval_my(val, tupdesc, "setdatabase", false, OIDOID));
            w.timeout = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "timeout", false, INT8OID));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "data", false, TEXTOID)), w.data, sizeof(w.data));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "user", false, TEXTOID)), w.user, sizeof(w.user));
            elog(LOG, "row = %lu, user = %s, data = %s, oid = %i, timeout = %li", row, w.user, w.data, w.oid, w.timeout);
            pg_quota_worker_start(&w);
        }
    } while (SPI_processed);
    SPI_cursor_close_my(portal);
    SPI_finish_my();
    pfree(src.data);
    set_ps_display_my("idle");
    if (!DatumGetBool(DirectFunctionCall2(pg_advisory_unlock_int4, Int32GetDatum(MyDatabaseId), Int32GetDatum(GetUserId())))) elog(WARNING, "!pg_advisory_unlock_int4(%i, %i)", MyDatabaseId, GetUserId());
}

void pg_quota_worker(Datum arg) {
    instr_time current_time_timeout;
    instr_time start_time_timeout;
    int64 timeout;
    long current_timeout = -1;
    Oid oid = DatumGetObjectId(arg);
#ifdef GP_VERSION_NUM
    Gp_role = GP_ROLE_UTILITY;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_UTILITY;
#endif
#endif
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionByOidMy(oid, InvalidOid);
    set_config_option_my("application_name", "pg_quota worker", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    pgstat_report_appname("pg_quota worker");
    set_ps_display_my("main");
    process_session_preload_libraries();
    if (!DatumGetBool(DirectFunctionCall2(pg_try_advisory_lock_int4, Int32GetDatum(MyDatabaseId), Int32GetDatum(GetUserId())))) { elog(WARNING, "!pg_try_advisory_lock_int4(%i, %i)", MyDatabaseId, GetUserId()); return; }
    timeout = atoll(GetConfigOption("pg_quota.timeout", false, true));
    elog(LOG, "oid = %i, timeout = %li", oid, timeout);
    set_ps_display_my("idle");
    while (!ShutdownRequestPending) {
        if (current_timeout <= 0) {
            INSTR_TIME_SET_CURRENT(start_time_timeout);
            current_timeout = timeout;
        }
        int rc = WaitLatchMy(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, timeout);
        if (rc & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
        if (rc & WL_LATCH_SET) pg_quota_latch();
        INSTR_TIME_SET_CURRENT(current_time_timeout);
        INSTR_TIME_SUBTRACT(current_time_timeout, start_time_timeout);
        current_timeout = timeout - (long)INSTR_TIME_GET_MILLISEC(current_time_timeout);
        if (current_timeout <= 0) pg_quota_timeout();
    }
    if (!DatumGetBool(DirectFunctionCall2(pg_advisory_unlock_int4, Int32GetDatum(MyDatabaseId), Int32GetDatum(GetUserId())))) elog(WARNING, "!pg_advisory_unlock_int4(%i, %i)", MyDatabaseId, GetUserId());
}
