package net.qihoo.ads.flink.functions.timestamps

import net.qihoo.ads.dw.dwd.max.MAXBidEvent
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor

class MAXBidEventTimestampsAndWatermarks(maxAllowedLateness: org.apache.flink.streaming.api.windowing.time.Time) extends BoundedOutOfOrdernessTimestampExtractor[MAXBidEvent](maxAllowedLateness) {

  override def extractTimestamp(element: MAXBidEvent): Long = {
    // us to ms
    if (element.getMaxTransactionBidRequestTime != null) element.getMaxTransactionBidRequestTime else 0
  }
}
