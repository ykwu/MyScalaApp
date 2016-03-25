/**
  * Created by wen on 3/25/16.
  */

package foo

import org.apache.spark.{SparkConf, SparkContext}
import org.mongodb.scala._
import Helpers._
import breeze.linalg.DenseMatrix

object MyApp {
    def main(args: Array[String]): Unit = {

        // MongoDB read and write
        //MongoTest()

        // use the command line to generate a jar file by typing "sbt package"
        // remember to comment out MongoTest
        //SparkTest()

        // Spark matrix operations using Breeze
        SparkMatrixTest()
    }

    def MongoTest(): Unit = {
        val mongoClient: MongoClient = MongoClient()
        val database: MongoDatabase = mongoClient.getDatabase("siwimi")
        val collection: MongoCollection[Document] = database.getCollection("Activity")

        // Test reading data operation
        collection.find().first().printHeadResult()


        collection.find().first().subscribe(
            (result: Document) => println("Success getting first record! " + result.toJson()),
            (error: Throwable) => println(s"Query failed: ${error.getMessage}"),
            () => println("Completed getting first record.")
        )


        // Test inserting data operation

        val doc: Document = Document("description" -> "MongoDB", "type" -> "database")
        val observable: Observable[Completed] = collection.insertOne(doc)

        observable.subscribe(new Observer[Completed] {

            override def onNext(result: Completed): Unit = println("Inserted one record")

            override def onError(e: Throwable): Unit = println("Failed inserting: " + e.getMessage())

            override def onComplete(): Unit = println("Completed inserting one record")
        })

        val query = Document("""{type: "database"}""")

        collection.find(query).subscribe(
            (result: Document) => println("Get inserted data back!" + result.toJson()),
            (error: Throwable) => println(s"Query failed: ${error.getMessage}"),
            () => println("Done getting inserted record")
        )

        mongoClient.close()
    }

    def SparkTest(): Unit = {
        val logFile = "/home/wen/idea-IC-145.258.11/LICENSE.txt" // Should be some file on your system
        val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)
        val logData = sc.textFile(logFile, 2).cache()
        val numAs = logData.filter(line => line.contains("a")).count()
        val numBs = logData.filter(line => line.contains("b")).count()
        println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    }

    def SparkMatrixTest(): Unit = {
        val n = 1000

        val a: DenseMatrix[Double] = DenseMatrix.rand(2, n)
        val b: DenseMatrix[Double] = DenseMatrix.rand(n, 3)
        val c = a * b
        println(s"Dot product of a and b is $c")
    }
}
