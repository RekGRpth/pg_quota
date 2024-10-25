#include "include.h"

#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/objectaccess.h>
#include <executor/executor.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <storage/shm_mq.h>
#include <storage/shm_toc.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#include <utils/relfilenodemap.h>

#if PG_VERSION_NUM < 130000
#include <catalog/pg_type.h>
#include <miscadmin.h>
#include <signal.h>
#endif

#define PG_QUOTA_MAGIC 0x70675F71
#define PG_QUOTA_QUEUE_SIZE (16 * 1024)

PG_MODULE_MAGIC;

typedef struct PgQuotaActiveTable {
    RelFileNode node; // ALWAYS FISRT !!!
    Oid relid;
} PgQuotaActiveTable;

typedef struct RejectMapEntry {
    Oid databaseoid;
    Oid tablespaceoid;
    Oid targetoid;
    RelFileNode relfilenode;
    uint32 targettype;
} RejectMapEntry;

typedef struct PgQuotaRejectTable {
    RejectMapEntry keyitem; // ALWAYS FISRT !!!
    bool segexceeded;
    RejectMapEntry auxblockinfo;
} PgQuotaRejectTable;

typedef struct PgQuotaWorker {
    char datname[NAMEDATALEN];
    char rolname[NAMEDATALEN];
    int64 timeout;
    Oid oid;
} PgQuotaWorker;

static bool pg_quota_hardlimit;
static ExecutorCheckPerms_hook_type prev_ExecutorCheckPerms_hook;
static file_create_hook_type prev_file_create_hook;
static file_extend_hook_type prev_file_extend_hook;
static file_truncate_hook_type prev_file_truncate_hook;
static file_unlink_hook_type prev_file_unlink_hook;
static HTAB *pg_quota_active_tables;
static HTAB *pg_quota_reject_tables;
static int pg_quota_launcher_fetch;
static int pg_quota_launcher_restart;
static int pg_quota_max_active_tables;
static int pg_quota_max_reject_tables;
static int pg_quota_worker_restart;
static LWLock *pg_quota_active_table_lock;
static LWLock *pg_quota_reject_table_lock;
static object_access_hook_type prev_object_access_hook;
static shmem_startup_hook_type prev_shmem_startup_hook;
static shm_mq *pg_quota_mq;

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

static List *pg_quota_get_index_list(Oid relid) {
    HeapTuple htup;
    List *result = NIL;
    Relation indrel = heap_open(IndexRelationId, AccessShareLock);
    ScanKeyData skey;
    ScanKeyInit(&skey, Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, relid);
    SysScanDesc indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true, NULL, 1, &skey);
    while (HeapTupleIsValid(htup = systable_getnext(indscan))) {
        Form_pg_index index = (Form_pg_index)GETSTRUCT(htup);
        if (!index->indislive) continue;
        result = lappend_oid(result, index->indexrelid);
    }
    systable_endscan(indscan);
    heap_close(indrel, AccessShareLock);
    return result;
}

static Size PgQuotaShmemSize(void)
{
    Size size = 0;
    size = add_size(size, hash_estimate_size(pg_quota_max_active_tables, sizeof(PgQuotaActiveTable)));
    size = add_size(size, hash_estimate_size(pg_quota_max_reject_tables, sizeof(PgQuotaRejectTable)));
    return size;
}

static void pg_quota_active_table_append(Oid relid, const RelFileNode *node) {
    bool found;
    PgQuotaActiveTable *entry;
    LWLockAcquire(pg_quota_active_table_lock, LW_EXCLUSIVE);
    if ((entry = hash_search(pg_quota_active_tables, node, hash_get_num_entries(pg_quota_active_tables) < pg_quota_max_active_tables ? HASH_ENTER : HASH_FIND, &found)) && relid != InvalidOid) entry->relid = relid;
    LWLockRelease(pg_quota_active_table_lock);
    elog(LOG, "found = %s", found ? "true" : "false");
}

static void pg_quota_active_table_remove(const RelFileNode *node) {
    bool found;
    LWLockAcquire(pg_quota_active_table_lock, LW_EXCLUSIVE);
    (void)hash_search(pg_quota_active_tables, node, HASH_REMOVE, &found);
    LWLockRelease(pg_quota_active_table_lock);
    elog(LOG, "found = %s", found ? "true" : "false");
}

static void export_exceeded_error(PgQuotaRejectTable *entry, bool skip_name) {
}

static void pg_quota_check_rejectmap_by_relfilenode(const RelFileNode *relfilenode) {
    if (!IsTransactionState()) return;
    if (!pg_quota_hardlimit) return;
    elog(LOG, "spcNode = %i, dbNode = %i, relNode = %i", relfilenode->spcNode, relfilenode->dbNode, relfilenode->relNode);
    bool found;
    RejectMapEntry keyitem = {.relfilenode = *relfilenode};
    LWLockAcquire(pg_quota_reject_table_lock, LW_SHARED);
    PgQuotaRejectTable *entry = hash_search(pg_quota_reject_tables, &keyitem, HASH_FIND, &found);
    if (found) {
        LWLockRelease(pg_quota_reject_table_lock);
        export_exceeded_error(entry, true);
        return;
    }
    LWLockRelease(pg_quota_reject_table_lock);
}

static void pg_quota_check_rejectmap_by_relid(Oid relid) {
    if (!IsTransactionState()) return;
    if (!OidIsValid(relid)) return;
    elog(LOG, "relid = %i", relid);
    Oid nsOid;
    Oid ownerOid;
    Oid tablespaceoid;
//    if (!get_rel_owner_schema_tablespace(relid, &ownerOid, &nsOid, &tablespaceoid)) return;
    LWLockAcquire(pg_quota_reject_table_lock, LW_SHARED);
//    for (QuotaType type = 0; type < NUM_QUOTA_TYPES; type++) {
        RejectMapEntry keyitem;
//        prepare_rejectmap_search_key(&keyitem, type, ownerOid, nsOid, tablespaceoid);
        bool found;
        PgQuotaRejectTable *entry = hash_search(pg_quota_reject_tables, &keyitem, HASH_FIND, &found);
        if (found) {
            LWLockRelease(pg_quota_reject_table_lock);
//            export_exceeded_error(entry, false);
            return;
        }
//    }
    LWLockRelease(pg_quota_reject_table_lock);
}

static bool pg_quota_ExecutorCheckPerms_hook(List *rangeTable, bool ereport_on_violation) {
    ListCell *lc;
    foreach (lc, rangeTable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
        if (rte->rtekind != RTE_RELATION) continue;
        if ((rte->requiredPerms & ACL_INSERT) == 0 && (rte->requiredPerms & ACL_UPDATE) == 0) continue;
        pg_quota_check_rejectmap_by_relid(rte->relid);
        List *indexIds = pg_quota_get_index_list(rte->relid);
        if (indexIds == NIL) continue;
        ListCell *oid;
        PG_TRY();
            foreach (oid, indexIds) pg_quota_check_rejectmap_by_relid(lfirst_oid(oid));
        PG_CATCH();
            list_free(indexIds);
            PG_RE_THROW();
        PG_END_TRY();
        list_free(indexIds);
    }
    if (prev_ExecutorCheckPerms_hook) return prev_ExecutorCheckPerms_hook(rangeTable, ereport_on_violation);
    return true;
}

static void pg_quota_file_create_hook(RelFileNodeBackend rnode) {
    if (prev_file_create_hook) prev_file_create_hook(rnode);
    Oid relid = RelidByRelfilenode(rnode.node.spcNode, rnode.node.relNode);
    elog(LOG, "relid = %i, spcNode = %i, dbNode = %i, relNode = %i", relid, rnode.node.spcNode, rnode.node.dbNode, rnode.node.relNode);
    pg_quota_active_table_append(relid, &rnode.node);
}

static void pg_quota_file_extend_hook(RelFileNodeBackend rnode) {
    if (prev_file_extend_hook) prev_file_extend_hook(rnode);
    Oid relid = RelidByRelfilenode(rnode.node.spcNode, rnode.node.relNode);
    elog(LOG, "relid = %i, spcNode = %i, dbNode = %i, relNode = %i", relid, rnode.node.spcNode, rnode.node.dbNode, rnode.node.relNode);
    pg_quota_active_table_append(relid, &rnode.node);
    pg_quota_check_rejectmap_by_relfilenode(&rnode.node);
}

static void pg_quota_file_truncate_hook(RelFileNodeBackend rnode) {
    if (prev_file_truncate_hook) prev_file_truncate_hook(rnode);
    Oid relid = RelidByRelfilenode(rnode.node.spcNode, rnode.node.relNode);
    elog(LOG, "relid = %i, spcNode = %i, dbNode = %i, relNode = %i", relid, rnode.node.spcNode, rnode.node.dbNode, rnode.node.relNode);
    pg_quota_active_table_append(relid, &rnode.node);
}

static void pg_quota_file_unlink_hook(RelFileNodeBackend rnode) {
    if (prev_file_unlink_hook) prev_file_unlink_hook(rnode);
    elog(LOG, "spcNode = %i, dbNode = %i, relNode = %i", rnode.node.spcNode, rnode.node.dbNode, rnode.node.relNode);
    pg_quota_active_table_remove(&rnode.node);
}

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
    worker.bgw_restart_time = pg_quota_launcher_restart;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (dynamic) {
        worker.bgw_notify_pid = MyProcPid;
        IsUnderPostmaster = true;
        if (!RegisterDynamicBackgroundWorker(&worker, NULL)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
        IsUnderPostmaster = false;
    } else RegisterBackgroundWorker(&worker);
}

static void pg_quota_object_access_hook_post_create(Oid relid) {
    Relation rel = RelationIdGetRelation(relid);
    elog(LOG, "relid = %i, spcNode = %i, dbNode = %i, relNode = %i", relid, rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode);
    pg_quota_active_table_append(relid, &rel->rd_node);
    RelationClose(rel);
}

static void pg_quota_object_access_hook(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg) {
    if (prev_object_access_hook) prev_object_access_hook(access, classId, objectId, subId, arg);
    if (classId != RelationRelationId) return;
    if (subId != 0) return;
    if (objectId < FirstNormalObjectId) return;
    switch (access) {
        case OAT_POST_CREATE: pg_quota_object_access_hook_post_create(objectId); break;
        default: break;
    }
}

static void pg_quota_worker_start(const PgQuotaWorker *w) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker = {0};
    pid_t pid;
    size_t len;
    set_ps_display_my("work");
    if ((len = strlcpy(worker.bgw_function_name, "pg_quota_worker", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_quota", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_quota worker", w->rolname, w->datname)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = ObjectIdGetDatum(w->oid);
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = pg_quota_worker_restart;
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

static Size pg_quota_shmem_size(void) {
    shm_toc_estimator e;
    shm_toc_initialize_estimator(&e);
    int nkeys = 1;
    shm_toc_estimate_chunk(&e, (Size) PG_QUOTA_QUEUE_SIZE);
    shm_toc_estimate_keys(&e, nkeys);
    Size size = shm_toc_estimate(&e);
    return size;
}

static void pg_quota_shmem_startup_hook(void) {
    if (prev_shmem_startup_hook) prev_shmem_startup_hook();
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
#if GP_VERSION_NUM >= 70000
    LWLockPadded *lock_base = GetNamedLWLockTranche("PgQuotaLocks");
    pg_quota_active_table_lock = &lock_base[0].lock;
    pg_quota_reject_table_lock = &lock_base[1].lock;
#else
    pg_quota_active_table_lock = LWLockAssign();
    pg_quota_reject_table_lock = LWLockAssign();
#endif
    {HASHCTL ctl = {
        .entrysize = sizeof(PgQuotaActiveTable),
#if GP_VERSION_NUM < 70000
        .hash = tag_hash,
#endif
        .keysize = sizeof(RelFileNode),
    };
    pg_quota_active_tables = ShmemInitHashMy("pg_quota_active_tables", pg_quota_max_active_tables, pg_quota_max_active_tables, &ctl, HASH_ELEM | HASH_FUNCTION);}
    {HASHCTL ctl = {
        .entrysize = sizeof(PgQuotaRejectTable),
#if GP_VERSION_NUM < 70000
        .hash = tag_hash,
#endif
        .keysize = sizeof(RejectMapEntry),
    };
    pg_quota_reject_tables = ShmemInitHashMy("pg_quota_reject_tables", pg_quota_max_reject_tables, pg_quota_max_reject_tables, &ctl, HASH_ELEM | HASH_FUNCTION);}
    bool found;
    Size segsize = pg_quota_shmem_size();
    void *sh = ShmemInitStruct("pg_quota", segsize, &found);
    if (!found) {
        shm_toc *toc = shm_toc_create(PG_QUOTA_MAGIC, sh, segsize);
        pg_quota_mq = shm_toc_allocate(toc, PG_QUOTA_QUEUE_SIZE);
        shm_toc_insert(toc, 0, pg_quota_mq);
    } else {
        shm_toc *toc = shm_toc_attach(PG_QUOTA_MAGIC, sh);
        pg_quota_mq = shm_toc_lookup(toc, 0, false);
    }
    LWLockRelease(AddinShmemInitLock);
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
    DefineCustomBoolVariable("pg_quota.hard_limit", "pg_quota hard limit", "Set this to 'on' to enable quota hardlimit.", &pg_quota_hardlimit, false, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_quota.launcher_fetch", "pg_quota launcher fetch", "Fetch launcher rows at once", &pg_quota_launcher_fetch, 10, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_quota.launcher_restart", "pg_quota launcher restart", "Restart launcher interval, seconds", &pg_quota_launcher_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_quota.max_active_tables", "pg_quota max active tables", "Max number of active tables monitored by pg_quota.", &pg_quota_max_active_tables, 300 * 1024, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_quota.max_reject_tables", "pg_quota max reject tables", "Max number of reject entries.", &pg_quota_max_reject_tables, 8192, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_quota.worker_restart", "pg_quota worker restart", "Restart worker interval, seconds", &pg_quota_worker_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    if (IsRoleMirror()) return;
    prev_ExecutorCheckPerms_hook = ExecutorCheckPerms_hook; ExecutorCheckPerms_hook = pg_quota_ExecutorCheckPerms_hook;
    prev_file_create_hook = file_create_hook; file_create_hook = pg_quota_file_create_hook;
    prev_file_extend_hook = file_extend_hook; file_extend_hook = pg_quota_file_extend_hook;
    prev_file_truncate_hook = file_truncate_hook; file_truncate_hook = pg_quota_file_truncate_hook;
    prev_file_unlink_hook = file_unlink_hook; file_unlink_hook = pg_quota_file_unlink_hook;
    prev_object_access_hook = object_access_hook; object_access_hook = pg_quota_object_access_hook;
    prev_shmem_startup_hook = shmem_startup_hook; shmem_startup_hook = pg_quota_shmem_startup_hook;
    RequestAddinShmemSpace(PgQuotaShmemSize());
#if GP_VERSION_NUM >= 70000
    RequestNamedLWLockTranche("PgQuotaLocks", 2);
#else
    RequestAddinLWLocks(2);
#endif
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
        ) SELECT "setdatabase", "datname"::text, "rolname"::text, ("setconfig"->>'pg_quota.timeout')::bigint AS "timeout"
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
        SPI_cursor_fetch(portal, true, pg_quota_launcher_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            PgQuotaWorker w = {0};
            set_ps_display_my("row");
            w.oid = DatumGetObjectId(SPI_getbinval_my(val, tupdesc, "setdatabase", false, OIDOID));
            w.timeout = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "timeout", false, INT8OID));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "datname", false, TEXTOID)), w.datname, sizeof(w.datname));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "rolname", false, TEXTOID)), w.rolname, sizeof(w.rolname));
            elog(LOG, "row = %lu, rolname = %s, datname = %s, oid = %i, timeout = %li", row, w.rolname, w.datname, w.oid, w.timeout);
            pg_quota_worker_start(&w);
            SPI_freetuple(val);
        }
        SPI_freetuptable(SPI_tuptable);
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
        int rc = WaitLatchMy(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, current_timeout);
        if (rc & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
        if (rc & WL_LATCH_SET) pg_quota_latch();
        INSTR_TIME_SET_CURRENT(current_time_timeout);
        INSTR_TIME_SUBTRACT(current_time_timeout, start_time_timeout);
        current_timeout = timeout - (long)INSTR_TIME_GET_MILLISEC(current_time_timeout);
        if (current_timeout <= 0) pg_quota_timeout();
    }
    if (!DatumGetBool(DirectFunctionCall2(pg_advisory_unlock_int4, Int32GetDatum(MyDatabaseId), Int32GetDatum(GetUserId())))) elog(WARNING, "!pg_advisory_unlock_int4(%i, %i)", MyDatabaseId, GetUserId());
}
