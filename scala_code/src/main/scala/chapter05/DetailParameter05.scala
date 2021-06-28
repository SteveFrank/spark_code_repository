package chapter05

/**
 * @date 2021/5/8
 */
object DetailParameter05 {

  def main(args: Array[String]): Unit = {
    mysqlCon(user = "tom", pwd = "123")
  }

  def mysqlCon(add: String = "localhost", port: Int = 3306,
               user: String = "root", pwd: String = "root"): Unit = {
    println(s"add = $add")
    println(s"port = $port")
    println(s"user = $user")
    println(s"pwd = $pwd")
  }

}
