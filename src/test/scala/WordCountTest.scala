import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.streaming.dstream.DStream

class WordCountTest extends TestSuiteBase {
  test("Simple input") {
    testOperation(
      Seq(
        Seq("cat", "dog"),
        Seq("dog dog", "sheep dog"),
        Seq("owl", "cow", "cat"),
        Seq("dog", "dog   cat", "dog")
      ),
      (s: DStream[String]) => WordCount(s),
      Seq(
        Seq(("cat", 1L), ("dog", 1L)),
        Seq(("dog", 3L), ("sheep", 1L)),
        Seq(("owl", 1L), ("cow", 1L), ("cat", 1L)),
        Seq(("dog", 3L), ("cat", 1L))),
      useSet = true
    )
  }
}
