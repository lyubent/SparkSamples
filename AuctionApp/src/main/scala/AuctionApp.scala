// MapR 2016
// AuctionsApp.scala - Simple App to inspect Auction data 
// The following import statements are importing SparkContext, all subclasses and SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object AuctionApp {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("AuctionsApp")
    val sc = new SparkContext(conf)

    // Add location of input file
    val aucFile = "/user/user01/data/auctiondata.csv"

    // declare field mapping
    val auctionid = 0
    val bid = 1
    val bidtime = 2
    val bidder = 3
    val bidderrate = 4
    val openbid = 5
    val price = 6
    val itemtype = 7
    val daystolive = 8

    //build the inputRDD
    val auctionRDD = sc.textFile(aucFile)
                       .map(_.split(","))
                       .cache()

    // total number of bids across all auctions
    val totalbids=auctionRDD.count()
    // total number of items (auctions)
    val totalitems=auctionRDD.map(line=>line(auctionid)).distinct().count()
    // RDD containing ordered pairs of auctionid,number
    val bids_auctionRDD=auctionRDD.map(line=>(line(auctionid),1)).reduceByKey((x,y)=> x+y)
    //max, min and avg number of bids
    val maxbids=bids_auctionRDD.map(x=>x._2).reduce((x,y)=>Math.max(x,y))
    val minbids=bids_auctionRDD.map(x=>x._2).reduce((x,y)=>Math.min(x,y))
    val avgbids=totalbids/totalitems

    println("total bids across all auctions: %s ".format(totalbids))
    println("total number of distinct items: %s ".format(totalitems))
    println("Max bids across all auctions: %s ".format(maxbids))
    println("Min bids across all auctions: %s ".format(minbids))
    println("Avg bids across all auctions: %s ".format(avgbids))
  }
}
