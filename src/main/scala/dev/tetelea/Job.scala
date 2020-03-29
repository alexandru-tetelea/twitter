package dev.tetelea


import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Job {
  val LOCAL_SAVE_PATH = "file-storage.output-path"

  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    println("Usage: TwitterExample " +
      "--bootstrap-servers <host:port>,<host:port> " +
      "--group-id <groupId> " +
      "--file-storage.output-path <path>")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    env.setParallelism(params.getInt("parallelism", 1))

    val streamSource: DataStream[String] =
      if (
        params.has("group-id") &&
          params.has("bootstrap-servers") &&
          params.has("topic") &&
          params.has(LOCAL_SAVE_PATH)
      ) {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", params.get("bootstrap-servers"))
        properties.setProperty("group.id", params.get("group-id"))
        val topic = params.get("topic")
        env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties))
      } else {
        print("Executing TwitterStream example with default props.")
        print("Use --bootstrap-servers <host:port>,<host:port> --topic <topic> " +
          "--group-id <groupId> " +
          "--file-storage.output-path <path>"
        )
        throw new RuntimeException
      }

    val outputPath = params.get(LOCAL_SAVE_PATH)
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 100)
          .build())
      .build()

    streamSource.addSink(sink)

    env.execute("Twitter File Sink!")
  }
}
