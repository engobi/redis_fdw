/*-------------------------------------------------------------------------
 *
 * redis_fdw.c
 *		  foreign-data wrapper for Redis
 *
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  redis_fdw/redis_fdw.c
 *
 * TODO:
 *	- Properly handle queries with quals
 *      - Handle Redis authentication
 *	- Allow the use of different Redis databases
 *-------------------------------------------------------------------------
 */

/* Debug mode */
/* #define DEBUG */

#include "postgres.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include <hiredis/hiredis.h>

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "optimizer/cost.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct RedisFdwOption
{
	const char	*optname;
	Oid		optcontext;	/* Oid of catalog in which option may appear */
};

/*
 * Valid options for redis_fdw.
 *
 */
static struct RedisFdwOption valid_options[] =
{

	/* Connection options */
	{ "address",		ForeignServerRelationId },
	{ "port",		ForeignServerRelationId },

	/* Sentinel */
	{ NULL,			InvalidOid }
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef struct RedisFdwExecutionState
{
	AttInMetadata	*attinmeta;
	redisContext	*context;
	redisReply	*reply;
	long long	row;
	char		*address;
	int		port;
} RedisFdwExecutionState;

/*
 * SQL functions
 */
extern Datum redis_fdw_handler(PG_FUNCTION_ARGS);
extern Datum redis_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(redis_fdw_handler);
PG_FUNCTION_INFO_V1(redis_fdw_validator);

/*
 * FDW callback routines
 */
static FdwPlan *redisPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
static void redisExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void redisBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *redisIterateForeignScan(ForeignScanState *node);
static void redisReScanForeignScan(ForeignScanState *node);
static void redisEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static bool redisIsValidOption(const char *option, Oid context);
static void redisGetOptions(Oid foreigntableid, char **address, int *port);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
redis_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

#ifdef DEBUG
	elog(NOTICE, "redis_fdw_handler");
#endif

	fdwroutine->PlanForeignScan = redisPlanForeignScan;
	fdwroutine->ExplainForeignScan = redisExplainForeignScan;
	fdwroutine->BeginForeignScan = redisBeginForeignScan;
	fdwroutine->IterateForeignScan = redisIterateForeignScan;
	fdwroutine->ReScanForeignScan = redisReScanForeignScan;
	fdwroutine->EndForeignScan = redisEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
redis_fdw_validator(PG_FUNCTION_ARGS)
{
	List		*options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid		catalog = PG_GETARG_OID(1);
	char		*svr_address = NULL;
	int		svr_port = 0;
	ListCell	*cell;

#ifdef DEBUG
	elog(NOTICE, "redis_fdw_validator");
#endif

	/*
	 * Check that only options supported by redis_fdw,
	 * and allowed for the current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem	   *def = (DefElem *) lfirst(cell);

		if (!redisIsValidOption(def->defname, catalog))
		{
			struct RedisFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
					                 opt->optname);
			}

			ereport(ERROR, 
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME), 
				errmsg("invalid option \"%s\"", def->defname), 
				errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
				));
		}

		if (strcmp(def->defname, "address") == 0)
		{
			if (svr_address)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
					errmsg("conflicting or redundant options: address (%s)", defGetString(def))
					));

			svr_address = defGetString(def);
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			if (svr_port)
				ereport(ERROR, 
					(errcode(ERRCODE_SYNTAX_ERROR), 
					errmsg("conflicting or redundant options: port (%s)", defGetString(def))
					));

			svr_port = atoi(defGetString(def));
		}
	}

	PG_RETURN_VOID();
}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
redisIsValidOption(const char *option, Oid context)
{
	struct RedisFdwOption *opt;

#ifdef DEBUG
	elog(NOTICE, "redisIsValidOption");
#endif

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a redis_fdw foreign table.
 */
static void
redisGetOptions(Oid foreigntableid, char **address, int *port)
{
	ForeignTable	*table;
	ForeignServer	*server;
	List            *options;
	ListCell        *lc;

#ifdef DEBUG
	elog(NOTICE, "redisGetOptions");
#endif

	/*
	 * Extract options from FDW objects.
	 * We only need to worry about server options for Redis
	 *
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);

	options = NIL;
	options = list_concat(options, server->options);

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "address") == 0)
			*address = defGetString(def);

		if (strcmp(def->defname, "port") == 0)
			*port = atoi(defGetString(def));
	}

	/* Default values, if required */
	if (!*address)
		*address = "127.0.0.1";

	if (!*port)
		*port = 6379;
}

/*
 * redisPlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
redisPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel)
{
	FdwPlan		*fdwplan;
	char		*svr_address = 0;
	int		svr_port = 0;
	redisContext	*context;
	redisReply	*reply;
	struct timeval  timeout = {1, 500000};

#ifdef DEBUG
	elog(NOTICE, "redisPlanForeignScan");
#endif

	/* Fetch options  */
	redisGetOptions(foreigntableid, &svr_address, &svr_port);

	/* Connect to the database */
	context = redisConnectWithTimeout(svr_address, svr_port, timeout);

	if (context->err)
		ereport(ERROR, 
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION), 
			errmsg("failed to connect to Redis: %d", context->err)
			));

	/* Execute a query to get the database size */
	reply = redisCommand(context, "DBSIZE");

	if (!reply)
	{
		redisFree(context);
		ereport(ERROR, 
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION), 
			errmsg("failed to get the database size: %d", context->err)
			));
	}

	/*
	 * Construct FdwPlan with cost estimates.
	 */
	fdwplan = makeNode(FdwPlan);

	/* Local databases are probably faster */
	if (strcmp(svr_address, "127.0.0.1") == 0 || strcmp(svr_address, "localhost") == 0)
		fdwplan->startup_cost = 10;
	else
		fdwplan->startup_cost = 25;

	baserel->rows = reply->integer;
	fdwplan->total_cost = reply->integer + fdwplan->startup_cost;
	fdwplan->fdw_private = NIL;	/* not used */

	freeReplyObject(reply);
	redisFree(context);

	return fdwplan;
}

/*
 * fileExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
redisExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	redisReply	*reply;

	RedisFdwExecutionState *festate = (RedisFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "redisExplainForeignScan");
#endif

	/* Execute a query to get the database size */
	reply = redisCommand(festate->context, "DBSIZE");

	if (!reply)
	{
		redisFree(festate->context);
		ereport(ERROR, 
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION), 
			errmsg("failed to get the database size: %d", festate->context->err)
			));
	}

	/* Suppress file size if we're not showing cost details */
	if (es->costs)
	{
		ExplainPropertyLong("Foreign Redis Database Size", reply->integer, es);
	}

	freeReplyObject(reply);
}

/*
 * redisBeginForeignScan
 *		Initiate access to the database
 */
static void
redisBeginForeignScan(ForeignScanState *node, int eflags)
{
	char			*svr_address = 0;
	int			svr_port = 0;
	redisContext		*context;
	redisReply		*reply;
	RedisFdwExecutionState	*festate;
	struct timeval		timeout = {1, 500000};

#ifdef DEBUG
	elog(NOTICE, "BeginForeignScan");
#endif

	/* Fetch options  */
	redisGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_address, &svr_port);

	/* Connect to the database */
	context = redisConnectWithTimeout(svr_address, svr_port, timeout);

	if (context->err)
	{
		redisFree(context);
		ereport(ERROR, 
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION), 
			errmsg("failed to connect to Redis: %d", context->err)
			));
	}

	/* Stash away the state info we have already */
	festate = (RedisFdwExecutionState *) palloc(sizeof(RedisFdwExecutionState));
	node->fdw_state = (void *) festate;
	festate->context = context;
	festate->row = 0;
	festate->address = svr_address;
	festate->port = svr_port;

	/* OK, we connected. If this is an EXPLAIN, bail out now */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Execute the query */
	reply = redisCommand(context, "KEYS *");

	if (!reply)
	{
		redisFree(festate->context);
		ereport(ERROR, 
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION), 
			errmsg("failed to list keys: %d", context->err)
			));
	}

	/* Store the additional state info */
	festate->attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
	festate->reply = reply;
}

/*
 * redisIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
redisIterateForeignScan(ForeignScanState *node)
{
	bool			found;
	redisReply		*reply = 0;
	char			*key;
	char 			*data = 0;
	char			**values;
	HeapTuple		tuple;

	RedisFdwExecutionState *festate = (RedisFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

#ifdef DEBUG
	elog(NOTICE, "redisIterateForeignScan");
#endif

	/* Cleanup */
	ExecClearTuple(slot);

	/* Get the next record, and set found */
	found = false;

	if (festate->row < festate->reply->elements)
	{
                /*
		 * Get the row, check the result type, and handle accordingly. 
                 * If it's nil, we go ahead and get the next row.
                 */
		do 
		{
			key = festate->reply->element[festate->row]->str;
			reply = redisCommand(festate->context, "GET %s", key);

			if (!reply)
			{
                        	freeReplyObject(festate->reply);
				redisFree(festate->context);
				ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY), 
					errmsg("failed to get the value for key \"%s\": %d", key, festate->context->err)
					));
			}

			festate->row++;

		} while ((reply->type == REDIS_REPLY_NIL ||
			  reply->type == REDIS_REPLY_STATUS ||
			  reply->type == REDIS_REPLY_ERROR) && 
			  festate->row < festate->reply->elements);

		if (festate->row <= festate->reply->elements)
		{
			/* 
		 	 * Now, deal with the different data types we might
			 * have got from Redis.
			 */

			switch (reply->type)
			{
				case REDIS_REPLY_INTEGER:
					data = (char *) palloc(sizeof(char) * 64);
					snprintf(data, 64, "%lld", reply->integer);
					found = true;
					break;

				case REDIS_REPLY_STRING:
					data = reply->str;
					found = true;
					break;

				case REDIS_REPLY_ARRAY:
					data = "<array>";
					found = true;
					break;
			}
		}

	}

	/* Build the tuple */
	values = (char **) palloc(sizeof(char *) * 2);

	if (found)
	{
		values[0] = key;
		values[1] = data;
		tuple = BuildTupleFromCStrings(festate->attinmeta, values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}

	/* Cleanup */
	if (reply)
		freeReplyObject(reply);

	return slot;
}

/*
 * redisEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
redisEndForeignScan(ForeignScanState *node)
{
	RedisFdwExecutionState *festate = (RedisFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "redisEndForeignScan");
#endif

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
	{
		if (festate->reply)
			freeReplyObject(festate->reply);

		if (festate->context)
			redisFree(festate->context);
	}
}

/*
 * redisReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
redisReScanForeignScan(ForeignScanState *node)
{
	RedisFdwExecutionState *festate = (RedisFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "redisReScanForeignScan");
#endif

	festate->row = 0;
}

