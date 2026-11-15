Security

- Errors, request logs and management quota-probe responses no longer show a password or query key embedded in an upstream URL. A URL in error text keeps its scheme, host, port and path, drops the userinfo and prints each query value and the fragment as `***`.
