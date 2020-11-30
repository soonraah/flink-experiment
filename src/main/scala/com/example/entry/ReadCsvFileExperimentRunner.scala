package com.example.entry

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.{PojoCsvInputFormat, TupleCsvInputFormat}
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.typeutils.{PojoField, PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
 * The experiment to read CSV file by Flink.
 * It reads CSV file as POJOs or tuples and just prints on console.
 */
object ReadCsvFileExperimentRunner {
  /** POJO */
  case class Company(name: String, ticker: String, numEmployees: Int)

  /** Tuple */
  type CompanyTuple = Tuple3[String, String, Int]

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val companiesFilePath = "data/companies.csv"
    val interval = 10.seconds

    args.headOption match {
      case None | Some("pojo") =>
        val inputFormat = createPojoCsvInputFormat(companiesFilePath)
        env
          .readFile(inputFormat, companiesFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, interval.toMillis)
          .map(_.toString)
          .print()
      case Some("tuple") =>
        val inputFormat = createTupleCsvInputFormat(companiesFilePath)
        env
          .readFile(inputFormat, companiesFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, interval.toMillis)
          .map(_.toString)
          .print()
      case _ =>
        throw new RuntimeException(s"Unsupported input format: ${args(0)}")
    }

    env.execute()
  }

  private def createPojoCsvInputFormat(csvFilePath: String): PojoCsvInputFormat[Company] = {
    val clazz = classOf[Company]
    val pojoFields = Seq(
      new PojoField(clazz.getDeclaredField("name"), BasicTypeInfo.STRING_TYPE_INFO),
      new PojoField(clazz.getDeclaredField("ticker"), BasicTypeInfo.STRING_TYPE_INFO),
      new PojoField(clazz.getDeclaredField("numEmployees"), BasicTypeInfo.INT_TYPE_INFO)
    ).asJava
    val pojoTypeInfo = new PojoTypeInfo[Company](clazz, pojoFields)
    val fieldNames = Array("name", "ticker", "numEmployees")

    val inputFormat = new PojoCsvInputFormat[Company](new Path(csvFilePath), pojoTypeInfo, fieldNames)
    inputFormat.setSkipFirstLineAsHeader(true)
    inputFormat
  }

  private def createTupleCsvInputFormat(csvFilePath: String): TupleCsvInputFormat[CompanyTuple] = {
    val types = Seq(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO
    )
    val tupleTypeInfo = new TupleTypeInfo[CompanyTuple](classOf[CompanyTuple], types: _*)

    val inputFormat = new TupleCsvInputFormat[CompanyTuple](new Path(csvFilePath), tupleTypeInfo)
    inputFormat.setSkipFirstLineAsHeader(true)
    inputFormat
  }
}
