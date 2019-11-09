import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

object CollectNeighborIds {
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

		val conf=new SparkConf()
				.setMaster("local[2]")
				.setAppName("CollectNeighborIds")
		val sc=new SparkContext(conf)

		//构造图
		val vertexNum = 629814
		val arrInt:Array[(Long, Int)] = new Array[(Long, Int)](vertexNum)
		for( a <- 0 until vertexNum){
			arrInt(a) = (a, 1)
		}
		val users: RDD[(VertexId, Int)] = sc.parallelize(arrInt)
		val relationships: RDD[Edge[Int]] = sc.textFile("/Users/wn/Project/GraphX/data/output.txt")
        		.map(line => Edge(line.split(" ")(0).toLong, line.split(" ")(1).toLong, 1))
		val graph = Graph(users, relationships)
		graph.cache()
		val graphNeighborUtil=new GraphNeighborUtil

		//获取二级邻居的ids
		val id = 109133
		val secondIds=graphNeighborUtil.getIds(id,graph)

		val graphXUtil=new GraphXUtil
		val vertices=graphXUtil.getSubGraphXVertices(graph,secondIds)
		val edges=graphXUtil.getSubGraphXEdges(graph,secondIds)

		val subGraph=Graph(vertices,edges)

		val firstNeighbor:VertexRDD[Double]=graph.pageRank(0.01).vertices

		val neighborRank = firstNeighbor.filter(pred=>{
			var flag=false
			secondIds.foreach(id=>if(id == pred._1) flag = true)
			flag
		}).sortBy(x=>x._2,false)          //按照rank从大到小排序
        		.map(t=>t._1+" "+t._2)
				.coalesce(1)
		neighborRank.cache()
		neighborRank.saveAsTextFile("/Users/wn/Project/GraphX/data/rank_"+id)

		println("second neighbor rank:")
		neighborRank.foreach(println)
		println("top five rank:")
		neighborRank.take(5).foreach(println)

		sc.stop()

	}

}
