import org.apache.flink.api.scala.ExecutionEnvironment
object WordCountBatch {
  def main(args: Array[String]): Unit = {

    val inputPath = "D:\\data\\11.txt"
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment
    //val env = ExecutionEnvironment.createLocalEnvironmentWithWebUI()
    import  org.apache.flink.api.scala._
    val text = env.readTextFile(inputPath)
    val counts = text.flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv("D:\\data\\output6").setParallelism(1)
    env.execute("batch wordCount")
  }

}