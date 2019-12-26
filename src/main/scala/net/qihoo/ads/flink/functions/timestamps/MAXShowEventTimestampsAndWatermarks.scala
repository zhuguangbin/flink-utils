package net.qihoo.ads.flink.functions.timestamps

import net.qihoo.ads.dw.dwd.max.MAXShowEvent
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor

class MAXShowEventTimestampsAndWatermarks(maxAllowedLateness: org.apache.flink.streaming.api.windowing.time.Time) extends BoundedOutOfOrdernessTimestampExtractor[MAXShowEvent](maxAllowedLateness) {

  override def extractTimestamp(element: MAXShowEvent): Long = {
    // us to ms
    if(element.getAudienceBehaviorWatchTimestamp != null) element.getAudienceBehaviorWatchTimestamp else 0
  }
}
