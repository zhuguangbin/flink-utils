## thrift-json format

in our production kafka, ad server engine log is thrift protocol. when using it as flink table, we can create table as thrift-json format for example:


```
CREATE TEMPORARY TABLE `ods_kfk_shbt2_e_c_6`(
	logId BIGINT,
	eventTime BIGINT,
	timestamps ROW<eventTimeType STRING, httpRequestTime BIGINT, exchangerRequestSendTime BIGINT, dspResponseRecvTime BIGINT, logGenerateTime BIGINT, showEventTime BIGINT>,
	eventType ROW<id INT, type INT, name STRING, adcoreVersion INT, flags BINARY>,
	seqId BIGINT,
	contextInfo ROW<deviceInfo ROW<platform STRING, brand STRING, model STRING, os STRING, osVersion STRING, network INTEGER, carrierId INTEGER, screenWidth INTEGER, screenHeight INTEGER>, params MAP<STRING, STRING>, ip ARRAY<INTEGER>, geoInfo ROW<country INTEGER, province INTEGER, city INTEGER, county INTEGER>, rawIp ARRAY<INTEGER>, userAgent STRING, acceptLanguage STRING, `language` STRING, userAgentInfo ROW<osId INTEGER, os STRING, browserId INTEGER, browser STRING>, hostname STRING>,
	advertisementInfo ROW<exchangeId STRING, exchangeBidId STRING, dspRequestId STRING, impressionInfos ARRAY<ROW<showInfo ROW<impressionId BIGINT, bidRequestLogId BIGINT,width INTEGER, height INTEGER, advertiserId INTEGER, campaignId INTEGER, solutionId INTEGER, creativeId INTEGER>>>, impressionInfo ROW<showInfo ROW<impressionId BIGINT, bidRequestLogId BIGINT,width INTEGER, height INTEGER, advertiserId INTEGER, campaignId INTEGER, solutionId INTEGER, creativeId INTEGER>>>,
	userBehavior ROW<urlInfo ROW<uri STRING, pageReferralUrl STRING>, clickEventId BIGINT, sourceId STRING>,
	logFlags BINARY,
	debugInfo BINARY,
	formatVersion INTEGER,
	TrafficMLInfo BINARY
) WITH (
  'connector'='kafka',
  'format'='thrift-json',
  'thrift-json.thrift-class'='com.mediav.data.log.unitedlog.UnitedEvent',
  'topic'='e.c.6',
  'properties.bootstrap.servers'='test-kf1.adsys.shbt2.qihoo.net:9092,test-kf2.adsys.shbt2.qihoo.net:9092,test-kf3.adsys.shbt2.qihoo.net:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'group-offsets',
  'scan.topic-partition-discovery.interval' = '1 min'
);
```

### TODO:

* thrift schema sometimes too complex, it is hard to determine field type. see the above example, `advertisermentInfo` field is a multi-layer Nested Row. we can build a tool for auto generate related flink table schema.
