import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object WordCountBatch {

  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    //val env = ExecutionEnvironment.createRemoteEnvironment("192.168.175.128",5006)
    val env = ExecutionEnvironment.getExecutionEnvironment
    //val  env =ExecutionEnvironment.createLocalEnvironment(2)
    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)
    val text =
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        println("Executing WordCount example with default input data set.")
        println("Use --input to specify file input.")
        env.readTextFile("D:\\data\\11.txt")
      }

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    if (params.has("output")) {
      counts.writeAsCsv(params.get("output"), "\n", " ")
      env.execute("Scala WordCount Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

  }
}
