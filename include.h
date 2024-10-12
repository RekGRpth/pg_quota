#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#define SQL(...) #__VA_ARGS__

#include <postgres.h>

#include <executor/spi.h>
#if PG_VERSION_NUM < 90500
#include <lib/stringinfo.h>
#endif
#include <libpq-fe.h>
#if PG_VERSION_NUM >= 160000
#include <nodes/miscnodes.h>
#endif

#ifdef GP_VERSION_NUM
#include <cdb/cdbvars.h>
#endif

#if PG_VERSION_NUM >= 90500
#define set_config_option_my(name, value, context, source, action, changeVal, elevel) set_config_option(name, value, context, source, action, changeVal, elevel, false)
#else
#define MyLatch (&MyProc->procLatch)
#define set_config_option_my(name, value, context, source, action, changeVal, elevel) set_config_option(name, value, context, source, action, changeVal, elevel)
#endif

#if PG_VERSION_NUM >= 110000
#define BackgroundWorkerInitializeConnectionMy(dbname, username) BackgroundWorkerInitializeConnection(dbname, username, 0)
#define BackgroundWorkerInitializeConnectionByOidMy(dboid, useroid) BackgroundWorkerInitializeConnectionByOid(dboid, useroid, 0)
#else
#define BackgroundWorkerInitializeConnectionMy(dbname, username) BackgroundWorkerInitializeConnection(dbname, username)
#define BackgroundWorkerInitializeConnectionByOidMy(dboid, useroid) BackgroundWorkerInitializeConnectionByOid(dboid, useroid)
extern void BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid);
#endif

#if PG_VERSION_NUM >= 130000
#define set_ps_display_my(activity) set_ps_display(activity)
#else
#define set_ps_display_my(activity) set_ps_display(activity, false)
#endif

Datum SPI_getbinval_my(HeapTuple tuple, TupleDesc tupdesc, const char *fname, bool allow_null, Oid typeid);
PGDLLEXPORT void pg_quota_launcher(Datum arg);
PGDLLEXPORT void pg_quota_worker(Datum arg);
Portal SPI_cursor_open_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls, bool read_only);
Portal SPI_cursor_open_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, bool read_only);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
void _PG_init(void);
void SPI_connect_my(const char *src);
void SPI_cursor_close_my(Portal portal);
void SPI_cursor_fetch_my(const char *src, Portal portal, bool forward, long count);
void SPI_execute_plan_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls, int res);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res);
void SPI_finish_my(void);

#endif // _INCLUDE_H_
