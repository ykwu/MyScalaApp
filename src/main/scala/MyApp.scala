/**
  * Created by wen on 3/25/16.
  * modified by Yi-Kuei on Apr 30, 2016
  */

package foo

import scala.util.matching.Regex

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._	// support mapRDD to map conversion
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable._
import scala.io.StdIn.{readLine, readInt}
import java.io.PrintWriter
import scala.math._
import Helpers._
import breeze.linalg.DenseMatrix

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat, BSONFileInputFormat, BSONFileOutputFormat}
import com.mongodb.hadoop.io.MongoUpdateWritable
import org.apache.hadoop.io._

import org.apache.spark.sql.SQLContext

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.log4j.Logger
import org.apache.log4j.Level

case class Activity(types: List[String])

object MyApp {
    def main(args: Array[String]): Unit = {
	// turn off the log
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)
        // MongoDB read and write
        //MongoTest()
	
	// regular expression
	val patternJson = new Regex(",*[a-zA-Z0-9]+,*")
	
	// the mongo-hadoop connector to get data into spark
	
	val mongoConfig = new Configuration()
	mongoConfig.set("mongo.input.uri", "mongodb://localhost:27017/siwimi.Activity")
	mongoConfig.set("mongo.input.query", "{type: {$ne: 'database'}}")
	//mongoConfig.set("mongo.input.query", "{zipCode: '48109'}")
	//mongoConfig.set("mongo.input.fields", "{title: 1, types: 1, zipCode: 1}")
	mongoConfig.set("mongo.input.fields", "{types: 1}")
	val Conf = new SparkConf().setMaster("local").setAppName("MyApp")
	Conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")	// set Kryo for serialization
	val sc = new SparkContext(Conf)
	// set connector for BSON to hadoop data
	val documents = sc.newAPIHadoopRDD(mongoConfig, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])	
	//val data = documents.map( doc => ( doc._1.toString(), doc._2.get("types").toString().replaceAll("""[ \p{Punct}&&[^,]]""", "") ) ).map( doc => (doc._1, doc._2.split(",").toList) ).map( doc => (doc._2, 1)).reduceByKey(_ + _).collect()
	val invDF = documents.map( doc => ( doc._2.get("types").toString().replaceAll("""[ \p{Punct}&&[^,]]""", "") ) ).flatMap( doc => doc.split(",") ).map(doc => (doc, 1)).reduceByKey(_ + _).map(doc => ( doc._1, 1.0/(doc._2) )).collectAsMap()
	
	val Category = invDF.keys.toSet
	val Categorybroadcast = sc.broadcast(Category)
	val invDFbroadcast = sc.broadcast(invDF.toMap)
	
	val data = documents.map( doc => ( doc._1.toString(), doc._2.get("types").toString().replaceAll("""[ \p{Punct}&&[^,]]""", "") ) ).map( doc =>  (doc._1, doc._2.split(",").toSet) ).map(doc => (doc._1, CategoryMatrix(doc._2,invDFbroadcast.value)) ).collect()

	//Category.foreach(println)
	//invDF.foreach(println)
	data.foreach(println)
	// remove broadcast variable, for rebroadcast
	invDFbroadcast.destroy()

	sc.stop // stop everything to avoid the ContextClean error

        // Spark matrix operations using Breeze
        //SparkMatrixTest()
    }
    
// define a function for converting list to map and then output a integer list
    def CategoryMatrix ( ASet:Set[String], invDF:Map[String, Double] ) : Any = {
	//val categories = List("parent", "museum", "art", "misc", "concert", "science", "birthday", "storytelling", "show", "playdate", "game", "movie", "zoo", "animal", "farm", "undefinetrue", "sport", "festival")
	val categories = invDF.keys.toList
	val zipList = List.fill(categories.length)(0.0)
	var newCatMap = (categories zip zipList).toMap
	var TFcount = ASet.size.toDouble
	for(x1 <- ASet.toIterator){ 
		// calculate TF-IDF
		newCatMap = newCatMap + (x1 -> invDF(x1) / sqrt(TFcount) )
	}
	//newCatMap
	val (newCatListKey, newCatListValue) = (newCatMap.unzip)
	newCatListValue
    }

// define matrix multiplication
    def SparkMatrixTest(): Unit = {
        val n = 1000

        val a: DenseMatrix[Double] = DenseMatrix.rand(2, n)
        val b: DenseMatrix[Double] = DenseMatrix.rand(n, 3)
        val c = a * b
        println(s"Dot product of a and b is $c")
    }
}
