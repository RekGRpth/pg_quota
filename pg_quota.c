#include <postgres.h>

#include <limits.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/ps_status.h>

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

PGDLLEXPORT void pg_quota_launcher(Datum arg);
void _PG_init(void);

static int pg_quota_launcher_restart;

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
    worker.bgw_restart_time = pg_quota_launcher_restart;
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
    DefineCustomIntVariable("pg_quota.launcher_restart", "pg_quota launcher restart", "Restart launcher interval, seconds", &pg_quota_launcher_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    pg_quota_init(false);
}

void pg_quota_launcher(Datum arg) {
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionMy("postgres", NULL);
    set_config_option_my("application_name", "pg_quota", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    pgstat_report_appname("pg_quota");
    set_ps_display_my("main");
    process_session_preload_libraries();
    if (!DatumGetBool(DirectFunctionCall2(pg_try_advisory_lock_int4, Int32GetDatum(MyDatabaseId), Int32GetDatum(GetUserId())))) { elog(WARNING, "!pg_try_advisory_lock_int4(%i, %i)", MyDatabaseId, GetUserId()); return; }
    if (!DatumGetBool(DirectFunctionCall2(pg_advisory_unlock_int4, Int32GetDatum(MyDatabaseId), Int32GetDatum(GetUserId())))) elog(WARNING, "!pg_advisory_unlock_int4(%i, %i)", MyDatabaseId, GetUserId());
}
