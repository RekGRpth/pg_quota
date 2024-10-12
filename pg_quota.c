#include <postgres.h>

#include <access/xact.h>
#include <commands/async.h>
#include <executor/spi.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/proc.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>

#define SQL(...) #__VA_ARGS__

#if PG_VERSION_NUM >= 90500
#define set_config_option_my(name, value, context, source, action, changeVal, elevel) set_config_option(name, value, context, source, action, changeVal, elevel, false)
#else
#define MyLatch (&MyProc->procLatch)
#define set_config_option_my(name, value, context, source, action, changeVal, elevel) set_config_option(name, value, context, source, action, changeVal, elevel)
#endif

#if PG_VERSION_NUM >= 110000
#define BackgroundWorkerInitializeConnectionMy(dbname, username) BackgroundWorkerInitializeConnection(dbname, username, 0)
#else
#define BackgroundWorkerInitializeConnectionMy(dbname, username) BackgroundWorkerInitializeConnection(dbname, username)
#endif

#if PG_VERSION_NUM >= 130000
#define set_ps_display_my(activity) set_ps_display(activity)
#else
#define set_ps_display_my(activity) set_ps_display(activity, false)
#endif

typedef enum STMT_TYPE {
    STMT_BIND,
    STMT_EXECUTE,
    STMT_FETCH,
    STMT_PARSE,
    STMT_STATEMENT,
} STMT_TYPE;

PGDLLEXPORT void pg_quota_launcher(Datum arg);
void _PG_init(void);

static bool was_logged;
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
    pg_quota_init(false);
}

static const char *stmt_type(STMT_TYPE stmt) {
    switch (stmt) {
        case STMT_BIND: return "bind";
        case STMT_EXECUTE: return "execute";
        case STMT_FETCH: return "fetch";
        case STMT_PARSE: return "parse";
        case STMT_STATEMENT: default: return "statement";
    }
}

static int errdetail_params_my(int nargs, Oid *argtypes, Datum *values, const char *nulls) {
    if (values && nargs > 0 && !IsAbortedTransactionBlockState()) {
        MemoryContext tmpCxt = AllocSetContextCreate(CurrentMemoryContext, "BuildParamLogString", ALLOCSET_DEFAULT_SIZES);
        MemoryContext oldcontext = MemoryContextSwitchTo(tmpCxt);
        StringInfoData buf;
        initStringInfo(&buf);
        for (int i = 0; i < nargs; i++) {
            appendStringInfo(&buf, "%s$%d = ", i > 0 ? ", " : "", i + 1);
            if ((nulls && nulls[i] == 'n') || !OidIsValid(argtypes[i])) appendStringInfoString(&buf, "NULL"); else {
                bool typisvarlena;
                char *pstring;
                Oid typoutput;
                getTypeOutputInfo(argtypes[i], &typoutput, &typisvarlena);
                pstring = OidOutputFunctionCall(typoutput, values[i]);
                appendStringInfoCharMacro(&buf, '\'');
                for (char *p = pstring; *p; p++)  {
                    if (*p == '\'') appendStringInfoCharMacro(&buf, *p);
                    appendStringInfoCharMacro(&buf, *p);
                }
                appendStringInfoCharMacro(&buf, '\'');
            }
        }
        errdetail("parameters: %s", buf.data);
        MemoryContextSwitchTo(oldcontext);
        MemoryContextDelete(tmpCxt);
    }
    return 0;
}

static void check_log_statement_my(STMT_TYPE stmt, const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, bool logged) {
    if (!logged) was_logged = false;
    else if (log_statement == LOGSTMT_NONE) was_logged = false;
    else if (log_statement == LOGSTMT_ALL) was_logged = true;
    else was_logged = false;
    debug_query_string = src;
    SetCurrentStatementStartTimestamp();
    if (!logged) ereport(DEBUG2, (errmsg("%s: %s", stmt_type(stmt), src), errhidestmt(true)));
    else if (was_logged) ereport(LOG, (errmsg("%s: %s", stmt_type(stmt), src), errhidestmt(true), errhidestmt(true), errdetail_params_my(nargs, argtypes, values, nulls)));
}

static void check_log_duration_my(STMT_TYPE stmt, const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls) {
    char msec_str[32];
    switch (check_log_duration(msec_str, was_logged)) {
        case 1: ereport(LOG, (errmsg("duration: %s ms", msec_str), errhidestmt(true))); break;
        case 2: ereport(LOG, (errmsg("duration: %s ms  %s: %s", msec_str, stmt_type(stmt), src), errhidestmt(true), errdetail_params_my(nargs, argtypes, values, nulls))); break;
    }
    debug_query_string = NULL;
    was_logged = false;
}

static Portal SPI_cursor_open_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, bool read_only) {
    Portal portal;
    SPI_freetuptable(SPI_tuptable);
    check_log_statement_my(STMT_BIND, src, nargs, argtypes, values, nulls, false);
    if (!(portal = SPI_cursor_open_with_args(NULL, src, nargs, argtypes, values, nulls, read_only, 0))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_cursor_open_with_args failed"), errdetail("%s", SPI_result_code_string(SPI_result)), errcontext("%s", src)));
    check_log_duration_my(STMT_BIND, src, nargs, argtypes, values, nulls);
    return portal;
}

static void SPI_connect_my(const char *src) {
    int rc;
    debug_query_string = src;
    pgstat_report_activity(STATE_RUNNING, src);
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_connect failed"), errdetail("%s", SPI_result_code_string(rc)), errcontext("%s", src)));
    PushActiveSnapshot(GetTransactionSnapshot());
    StatementTimeout > 0 ? enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout) : disable_timeout(STATEMENT_TIMEOUT, false);
}

static void SPI_cursor_close_my(Portal portal) {
    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_close(portal);
}

static void SPI_finish_my(void) {
    int rc;
    disable_timeout(STATEMENT_TIMEOUT, false);
    PopActiveSnapshot();
    if ((rc = SPI_finish()) != SPI_OK_FINISH) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_finish failed"), errdetail("%s", SPI_result_code_string(rc))));
#if PG_VERSION_NUM < 150000
    ProcessCompletedNotifies();
#endif
    CommitTransactionCommand();
    was_logged = false;
    pgstat_report_stat(false);
    debug_query_string = NULL;
    pgstat_report_activity(STATE_IDLE, NULL);
}

static Datum SPI_getbinval_my(HeapTuple tuple, TupleDesc tupdesc, const char *fname, bool allow_null, Oid typeid) {
    bool isnull;
    Datum datum;
    int fnumber = SPI_fnumber(tupdesc, fname);
    if (SPI_gettypeid(tupdesc, fnumber) != typeid) ereport(ERROR, (errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH), errmsg("type of column \"%s\" must be \"%i\"", fname, typeid)));
    datum = SPI_getbinval(tuple, tupdesc, fnumber, &isnull);
    if (allow_null) return datum;
    if (isnull) ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("column \"%s\" must not be null", fname)));
    return datum;
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
