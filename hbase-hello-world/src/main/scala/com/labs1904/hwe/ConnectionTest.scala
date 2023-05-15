package com.labs1904.hwe

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Delete, Get, Put, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}

import java.util
import scala.collection.JavaConverters._
object ConnectionTest {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)


  def main(args: Array[String]) = {

    var connection: Connection = null
    try {

      // config stuff
      logger.debug("Starting app")
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "CHANGEME")
      connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf("nrafferty:users"))

      //default stuff
//      val get = new Get(Bytes.toBytes("rowkey"))
//      val result = table.get(get)
//      val message = Bytes.toString(result.getFamilyMap(Bytes.toBytes("f1")).get(Bytes.toBytes("test")))
//      logger.debug(message)


      // challenge 1 get
//      val get = new Get(Bytes.toBytes("10000001"))
//      val result = table.get(get)
//      val message = Bytes.toString(result.getFamilyMap(Bytes.toBytes("f1")).get(Bytes.toBytes("mail")))
//      logger.debug(message)


      // challenge 2 put
//      val put = new Put(Bytes.toBytes("99"))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("username"), Bytes.toBytes("DE-HWE"))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("The Panther"))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("sex"), Bytes.toBytes("F"))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("favorite_color"), Bytes.toBytes("pink"))
//      table.put(put)
//      logger.debug("put!")


      // get challenge 2 data
//      val get = new Get(Bytes.toBytes("99"))
//      val result = table.get(get)
//      var message = Bytes.toString(result.getFamilyMap(Bytes.toBytes("f1")).get(Bytes.toBytes("username")))
//      logger.debug(message)
//      message = Bytes.toString(result.getFamilyMap(Bytes.toBytes("f1")).get(Bytes.toBytes("name")))
//      logger.debug(message)
//      message = Bytes.toString(result.getFamilyMap(Bytes.toBytes("f1")).get(Bytes.toBytes("sex")))
//      logger.debug(message)
//      message = Bytes.toString(result.getFamilyMap(Bytes.toBytes("f1")).get(Bytes.toBytes("favorite_color")))
//      logger.debug(message)

      // challenge 3 scan
//      val scan = new Scan().withStartRow(Bytes.toBytes("10000001")).withStopRow(Bytes.toBytes("10006001"))
//      scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"))
//      val scanner = table.getScanner(scan)
//      logger.debug(scanner.asScala.size)

      // challenge 4 delete
//      val delete = new Delete(Bytes.toBytes("99"))
//      table.delete(delete)
//      logger.debug("deleted!")


      // todo challenge 5 get multiple
      val get1 = new Get(Bytes.toBytes("9005729"))
      val get2 = new Get(Bytes.toBytes("500600"))
      val get3 = new Get(Bytes.toBytes("30059640"))
      val get4 = new Get(Bytes.toBytes("6005263"))
      val get5 = new Get(Bytes.toBytes("800182"))
      val results = table.get(List(get1, get2, get3, get4, get5).asJava)
      results.foreach(r => {
        logger.debug(Bytes.toString(r.getFamilyMap(Bytes.toBytes("f1")).get(Bytes.toBytes("mail"))))
      })


    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }


  }
}
