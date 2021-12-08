## json-in-tsv format

in our production kafka, there are some log events which are not normal json. for example:

```
20211208130916	DjSearchPv	{"pvid":"ff3d7b0462f79749","province":31,"city":328,"pv_src":"pcmax","src":"lm","fr":"","ls":"sn2359116","query":"\u7f8e\u98df\u98df\u8c31","now":1638940156,"pn":1,"apitype":2,"channel":113,"buckettest":0,"new_style":0,"source_type":3,"lmid":"ff3d7b0462f79749","ctype":22,"intent_province":"10001","intent_city":"10001","ins":0,"mid":"2218ce055b199596f53a0b6c72588840","huid":"","midpage_search":0,"parent_pvid":"","middle_pos":0,"lm_extend":"ctype:22|lmbid:27,18,37,34,66,72,82,94,103,110,500|jt:1|maxbid:4390929,4390931,4390945,4390951,4390978,4390996,4391011,4391026,4391042,4391463,4391488,4391504,4391537,4391552,4456453,4456457,4456713,4456963,4457016,4457029,4457058,4457062,4457064,4457072,4457074|sadspace:","req":false,"left_ad":{"2":{"aid":8820981285,"gid":811256389,"pid":44631559,"uid":3139669614,"bidword":"\u70e4\u7bb1\u98df\u8c31\u5927\u5168","bidtype":0,"bidprice":0.3,"cprice":0.25,"matchtype":2,"chan":113,"place":746,"subver":"","biyiimg":"","biyitext":"","sub_ad_info":[],"is_intent":0,"style_id":0,"ad_style_id":0,"sub_bidtype":0,"sub_matchtype":3,"strategy_id":0,"strategy_flag":0,"mp_channel":0,"keyword_id":44323091844,"recommend_query":"","personalised_query":"","p_var1":"","creativeitem":0,"materials":"","unify_style":"20000","recall_type":0,"skulist":"","good_pkg_id":0,"ocpc_id":0,"ocpc_stage":0,"ocpc_remark":"","pcvr":0,"ocpc_ext_id":0,"ocpc_expand_type":0,"showpos":"","styleid_premium":0,"styleid_premium_final":0,"ocpc_convertion_type_deep":"","deep_ocpc_coefficient":1,"show_ad_flag":"-1","plantype":2,"crowd_id":0,"crowd_expand":0,"crowd_expand_type":0,"crowd_ratio":1,"crowd_pkg_id":0}},"right_ad":[],"ip":"223.117.129.93","ip_v6":"","guid":"","soguid":"","sid":"ff3d7b0462f79749","so_ab_test":"","so_eci":"","so_nlpv":"","call_host":"shbt","bucketlist":"","client_ip":"223.117.129.93","client_ip_v6":"","_route":"pcmax\/rec","_log_host":"front03.adsys.shyc2.qihoo.net"}
20211208130916	DjSearchPv	{"pvid":"9d1e87e1fe5b1a41","province":4,"city":10001,"pv_src":"pcmax","src":"lm","fr":"","ls":"s6d34454c99","query":"\u6587\u7269\u9274\u5b9a\u4e0e\u9274\u8d4f","now":1638940156,"pn":1,"apitype":2,"channel":113,"buckettest":0,"new_style":0,"source_type":3,"lmid":"9d1e87e1fe5b1a41","ctype":20,"intent_province":"10001","intent_city":"10001","ins":0,"mid":"189ad379f66903c2e42c672719c362eb","huid":"","midpage_search":0,"parent_pvid":"","middle_pos":0,"lm_extend":"ctype:20|lmbid:20,17,3,4,62,78,82,94,107,111,500|jt:1|maxbid:4456713,4457015,4457029,4457064,4457074,4390929,4390930,4390944,4390951,4390978,4390996,4391011,4391026,4391042,4391463,4391488,4391504,4391537,4391552,4457057,4457062|sadspace:2360555","req":false,"left_ad":{"3":{"aid":263774556,"gid":41862555,"pid":14511553,"uid":27593408,"bidword":"\u6587\u7269\u9274\u5b9a\u4e0e\u9274\u8d4f","bidtype":0,"bidprice":3,"cprice":2.2,"matchtype":1,"chan":113,"place":746,"subver":"","biyiimg":"","biyitext":"","sub_ad_info":[],"is_intent":0,"style_id":0,"ad_style_id":0,"sub_bidtype":0,"sub_matchtype":0,"strategy_id":0,"strategy_flag":0,"mp_channel":0,"keyword_id":1279908979,"recommend_query":"","personalised_query":"","p_var1":"","creativeitem":0,"materials":"","unify_style":"20000","recall_type":0,"skulist":"","good_pkg_id":0,"ocpc_id":0,"ocpc_stage":0,"ocpc_remark":"","pcvr":0,"ocpc_ext_id":0,"ocpc_expand_type":0,"showpos":"","styleid_premium":0,"styleid_premium_final":0,"ocpc_convertion_type_deep":"","deep_ocpc_coefficient":1,"show_ad_flag":"-1","plantype":2,"crowd_id":0,"crowd_expand":0,"crowd_expand_type":0,"crowd_ratio":1,"crowd_pkg_id":0}},"right_ad":[],"ip":"117.59.39.130","ip_v6":"","guid":"","soguid":"15484592.4242541729208528400.1613476516986.4404","sid":"9d1e87e1fe5b1a41","so_ab_test":"","so_eci":"","so_nlpv":"","call_host":"shbt","bucketlist":"","client_ip":"117.59.39.130","client_ip_v6":"","_route":"pcmax\/rec","_log_host":"front03.adsys.shyc2.qihoo.net"}
20211208130916	DjSearchPv	{"pvid":"5c7b59fd15e2ff70","province":20,"city":228,"pv_src":"pcmax","src":"lm","fr":"","ls":"s6d34454c99","query":"\u6c7d\u8f66\u4ef7\u683c\u5927\u5168","now":1638940156,"pn":1,"apitype":2,"channel":113,"buckettest":0,"new_style":0,"source_type":3,"lmid":"5c7b59fd15e2ff70","ctype":20,"intent_province":"10001","intent_city":"10001","ins":0,"mid":"9846c9aa93908d9f720ad1a0cb7ba8d3","huid":"","midpage_search":0,"parent_pvid":"","middle_pos":0,"lm_extend":"ctype:20|lmbid:51|jt:1|maxbid:4456705,4456713,4456963,4457016,4457029,4457064,4457073,4390929,4390930,4390945,4390949,4390978,4390996,4391009,4391011,4391026,4391040,4391463,4391488,4391504,4391537,4391552,4457058,4457062|sadspace:2360555","req":false,"left_ad":{"2":{"aid":8845263308,"gid":812618046,"pid":44663350,"uid":3334590369,"bidword":"\u5c3c\u6851\u6c7d\u8f66\u62a5\u4ef7\u53ca\u56fe\u7247\u5927\u5168","bidtype":0,"bidprice":1.21,"cprice":1.02,"matchtype":2,"chan":113,"place":746,"subver":"","biyiimg":"","biyitext":"","sub_ad_info":[],"is_intent":0,"style_id":0,"ad_style_id":0,"sub_bidtype":0,"sub_matchtype":3,"strategy_id":0,"strategy_flag":0,"mp_channel":0,"keyword_id":44500526792,"recommend_query":"","personalised_query":"","p_var1":"","creativeitem":0,"materials":"","unify_style":"20000","recall_type":0,"skulist":"","good_pkg_id":0,"ocpc_id":0,"ocpc_stage":0,"ocpc_remark":"","pcvr":0,"ocpc_ext_id":0,"ocpc_expand_type":0,"showpos":"","styleid_premium":0,"styleid_premium_final":0,"ocpc_convertion_type_deep":"","deep_ocpc_coefficient":1,"show_ad_flag":"-1","plantype":2,"crowd_id":0,"crowd_expand":0,"crowd_expand_type":0,"crowd_ratio":1,"crowd_pkg_id":0}},"right_ad":[],"ip":"183.236.120.2","ip_v6":"","guid":"","soguid":"15484592.3885558667799917000.1624350644025.623","sid":"5c7b59fd15e2ff70","so_ab_test":"","so_eci":"","so_nlpv":"","call_host":"shyc2","bucketlist":"","client_ip":"183.236.120.2","client_ip_v6":"","_route":"pcmax\/rec","_log_host":"front03.adsys.shyc2.qihoo.net"}
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
  'properties.bootstrap.servers'='test-kf1.adsys.shbt2.qihoo.net:9092,test-kf2.adsys.shbt2.qihoo.net:9092,test-kf3.adsys.shbt2.qihoo.net:9092',
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
