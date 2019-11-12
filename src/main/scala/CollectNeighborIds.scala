import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import com.mongodb.spark._
import org.apache.spark.sql.SparkSession
import org.bson.Document

object CollectNeighborIds {
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

		val sparkSession = SparkSession.builder()
				.master("local[2]")
				.appName("CollectNeighborIds")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/db.GraphX")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db.rank")
				.getOrCreate()

		val sc=sparkSession.sparkContext

		//构造图
		val vertexNum = 629814
		val arrInt:Array[(Long, Int)] = new Array[(Long, Int)](vertexNum)
		for( a <- 0 until vertexNum){
			arrInt(a) = (a, 1)
		}
		val users: RDD[(VertexId, Int)] = sc.parallelize(arrInt)
		val relationships: RDD[Edge[Int]] = MongoSpark.load(sparkSession).rdd
				.map(row => Edge(row.getString(2).toLong, row.getString(1).toLong, 1))
		val graph = Graph(users, relationships)
		graph.cache()
		val graphNeighborUtil=new GraphNeighborUtil

		//获取二级邻居的ids
		val id = 56
		val secondIds=graphNeighborUtil.getIds(id,graph)

		val graphXUtil=new GraphXUtil
		val vertices=graphXUtil.getSubGraphXVertices(graph,secondIds)
		val edges=graphXUtil.getSubGraphXEdges(graph,secondIds)

		val subGraph=Graph(vertices,edges)

		val firstNeighbor:VertexRDD[Double]=graph.pageRank(tol=0.01).vertices

		val neighborRank = firstNeighbor.filter(pred=>{
			var flag=false
			secondIds.foreach(id=>if(id == pred._1) flag = true)
			flag
		}).sortBy(x=>x._2,ascending=false)          //按照rank从大到小排序
				.coalesce(numPartitions=1)

		val rankDocuments = neighborRank.map(e => Document.parse(s"{v: ${e._1}, r: ${e._2}}"))
		MongoSpark.save(rankDocuments)

		val rankString = neighborRank.map(t=>t._1+" "+t._2)
		rankString.saveAsTextFile("/Users/wn/Project/GraphX/data/rank_"+id)

		println("top ten rank:")
		rankString.take(10).foreach(println)

		sc.stop()

	}

}
