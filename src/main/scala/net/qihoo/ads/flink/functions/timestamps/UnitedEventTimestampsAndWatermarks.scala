package net.qihoo.ads.flink.functions.timestamps

import com.mediav.data.log.unitedlog.UnitedEvent
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor

class UnitedEventTimestampsAndWatermarks(maxAllowedLateness: org.apache.flink.streaming.api.windowing.time.Time) extends BoundedOutOfOrdernessTimestampExtractor[UnitedEvent](maxAllowedLateness) {

  override def extractTimestamp(element: UnitedEvent): Long = {
    // ms
    element.eventTime
  }
}
