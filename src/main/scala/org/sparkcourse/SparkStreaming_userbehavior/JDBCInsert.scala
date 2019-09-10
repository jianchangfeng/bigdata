package org.sparkcourse.SparkStreaming_userbehavior

object JDBCInsert {
  def main(args: Array[String]): Unit = {
    val conn = DBConnectionPool.getConnection();
    val sql = "insert into result(uid, click_count) values('" + "test" + "'," + 11 + ")"
    val stmt = conn.createStatement
    stmt.executeUpdate(sql)
    DBConnectionPool.returnConnection(conn)
  }
}
