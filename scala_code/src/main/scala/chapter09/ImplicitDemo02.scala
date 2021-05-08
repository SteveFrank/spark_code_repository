package chapter09

/**
 * @author yangqian
 * @date 2021/5/8
 */
object ImplicitDemo02 {

  // 使用隐式转换的方式动态给MySQL类增加delete方法

  def main(args: Array[String]): Unit = {

    implicit def addDelete(mysql: MySQL): DB = {
      new DB
    }
    //创建 mysql 对象
    val mySQL = new MySQL
    mySQL.insert()
    mySQL.delete() //  编译器工作 分析 addDelete$1(mySQL).delete()
    mySQL.update()
  }

}

class MySQL {
  def insert(): Unit = {
    println("MySQL.insert")
  }
}

class DB {
  def delete(): Unit = {
    println("DB.delete")
  }
  def update(): Unit = {
   println("DB.update")
  }
}
