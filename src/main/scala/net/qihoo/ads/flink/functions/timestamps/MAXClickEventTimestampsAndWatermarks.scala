package net.qihoo.ads.flink.functions.timestamps

import net.qihoo.ads.dw.dwd.max.MAXClickEvent
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor

class MAXClickEventTimestampsAndWatermarks(maxAllowedLateness: org.apache.flink.streaming.api.windowing.time.Time) extends BoundedOutOfOrdernessTimestampExtractor[MAXClickEvent](maxAllowedLateness) {

  override def extractTimestamp(element: MAXClickEvent): Long = {
    // us to ms
    if(element.getAudienceBehaviorClickTimestamp != null) element.getAudienceBehaviorClickTimestamp else 0
  }
}
