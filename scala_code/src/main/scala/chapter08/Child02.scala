package chapter08

/**
 * @author yangqian
 * @date 2021/5/8
 */
class Child02 (cName: String) {
  var name: String = cName
}

object Child02 {
  // 统计小孩总数
  var totalChildNum = 0

  def joinGame(child02: Child02): Unit = {
    printf(s"${child02.name} 小孩加入了游戏 \n")
    totalChildNum += 1
  }

  def showNum() = printf(s"当前有${this.totalChildNum}小孩加入游戏 \n")

}
