/*
Package accesslog replicates access log format supported by Envoy.

In order to allow users of Kuma to reuse the same access log format strings
both in file logs and TCP logs, we need to have native support for
Envoy access log command syntax.

Use ParseFormat() function to parse a format string.

Use LogConfigurer interface to configure `envoy.http_grpc_access_log` and `envoy.tcp_grpc_access_log`.

Use LogEntryFormatter interface to format HTTP and TCP log entries.

The initial implementation misses the following features:
1. `%START_TIME%` commands ignore a format string that could have been defined by a user
2. `%DYNAMIC_METADATA(NAMESPACE:KEY*):Z%` commands return a stub value
3. `%FILTER_STATE(KEY):Z%` commands return a stub value
*/
package accesslog
