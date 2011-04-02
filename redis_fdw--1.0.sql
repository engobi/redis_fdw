/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for Redis
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Dave Page <dpage@pgadmin.org>
 *
 * IDENTIFICATION
 *                redis_fdw/redis_fdw--1.0.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION redis_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION redis_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER redis_fdw
  HANDLER redis_fdw_handler
  VALIDATOR redis_fdw_validator;
