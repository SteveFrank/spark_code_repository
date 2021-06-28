package chapter08

/**
 * @date 2021/5/8
 */
class Pig(pName: String) {
  var name: String = pName
}

object Pig {
  def apply(pName: String): Pig = new Pig(pName=pName)
  def apply(): Pig = new Pig("测试")
}
