## json-in-tsv format

in our production kafka, there are some log events which are not normal json. for example:

```
field1_str  field2_str {JSON}
```

It is actually tsv format, but the most useful json message is one of it's field. we call it a json-in-tsv format.

## Flink Usage

```
CREATE TEMPORARY TABLE `flink_rtdw`.`hdp_ads_dw`.`ods_kfk_shbt2_dianjing_search_pv` (
  `pvid` STRING,
  `province` STRING,
  `city` STRING,
  `pv_src` STRING,
  `src` STRING,
  `fr` STRING,
  `ls` STRING,
  `query` STRING,
  `now` INTEGER,
  `pn` INTEGER,
  `apitype` INTEGER,
  `channel` INTEGER,
  `buckettest` INTEGER,
  `new_style` INTEGER,
  `source_type` INTEGER,
  `ctype` BIGINT,
  `ins` INTEGER,
  `intent_province` STRING,
  `intent_city` STRING,
  `mid` STRING,
  `huid` STRING,
  `parent_pvid` STRING,
  `lm_extend` STRING,
  `req` STRING,
  `left_ad` STRING,
  `left_bottom_ad` STRING,
  `ip` STRING,
  `ip_v6` STRING,
  `guid` STRING,
  `soguid` STRING,
  `sid` STRING,
  `so_ab_test` STRING,
  `so_eci` STRING,
  `so_nlpv` STRING,
  `call_host` STRING,
  `bucketlist` STRING,
  `client_ip` STRING,
  `client_ip_v6` STRING,
  `_route` STRING,
  `_log_host` STRING,
  `ets` AS TO_TIMESTAMP(FROM_UNIXTIME(now))
) WITH (
  'connector'='kafka',
  'format'='json-in-tsv',
  'json-in-tsv.json-field-index'='2',
  'topic'='DjSearchPv',
  'properties.bootstrap.servers'='YOUR-KAFKA-BOOTSTRAP-SERVERS',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'group-offsets',
  'scan.topic-partition-discovery.interval' = '1 min'
);
```

### format configuration:

```
field-delimiter: Optional field delimiter character ('	' by default)
line-charset: Optional charset of line ('UTF-8' by default)
json-field-index: Required, json field index, 0-based
```
