# spring-session-cassandra #

Support for using Cassandra as HTTP session storage for spring-boot.
The work is based in the previous work done by [@Fitzoh](https://github.com/Fitzoh) in [https://github.com/Fitzoh/spring-session](https://github.com/Fitzoh/spring-session). Here I just made the code available as a independent module.

### Usage ###
To use this module you just need to follow these steps:
* Configure a Cassandra database using the configuration for spring-data-cassandra.
* Create the following tables in your Cassandra database:

```sql
CREATE TABLE session (
    id uuid PRIMARY KEY,
    attributes map<text, text>,
    creation_time bigint,
    last_accessed bigint,
    max_inactive_interval_in_seconds int
);

CREATE TABLE session_by_name (
    principal_name text,
    id uuid,
    PRIMARY KEY (principal_name, id)
)
```
