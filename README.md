# TimescalePOC
This project is a timescale db POC with timeseries data

These commands build all the image needed and run the containers.
Wait until all the containers are in running status.

```shell

$ docker compose up 
```

Compose will also start a container with a script to populate the database
The parameters inside the compose are those that generate the critical scenario

This is the query that takes a long time
```shell

SELECT
COALESCE(SUM(t."Value"), 0.0) AS "Value",
NULL AS "LastCompleteTime",
t."Key" AS "Time"
FROM (
    SELECT
    c."Value",
    DATE_TRUNC('day', c."Date"::timestamp) AS "Key"
    FROM
    "Values" AS c
    WHERE
    c."Device" = ANY (ARRAY['1', '2', ..., '300']) -- full DeviceID list
    AND c."Tag" = 'Temperature'
    AND c."Date"::timestamp >= '2023-01-01 00:00:00'
    AND c."Date"::timestamp <= '2023-12-31 23:59:59'
) AS t
GROUP BY
t."Key"
ORDER BY
t."Key";
```
