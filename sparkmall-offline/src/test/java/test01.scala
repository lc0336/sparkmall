import scala.collection.mutable.ArrayBuffer

object Test1 {
  def main(args: Array[String]): Unit = {
    val sentence = "AAAAAAAAAABBBBBBBBCCCCCDDDDDDD"
    var arr: ArrayBuffer[Char] = new ArrayBuffer[Char]

    sentence.foldLeft(arr)(putArray)
    println(arr)
  }

  def putArray(arr: ArrayBuffer[Char], c: Char): ArrayBuffer[Char] = {
    arr.append(c)
    arr
  }
}
