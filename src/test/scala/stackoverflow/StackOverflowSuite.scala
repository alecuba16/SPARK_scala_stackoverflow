package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.Assert.assertEquals
import java.io.File

object StackOverflowSuite {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)
}

class StackOverflowSuite {
  import StackOverflowSuite._

  var posts:Array[Posting] = new Array[Posting](6)
  posts(0)=new Posting(1,0, None,None,0,Some(testObject.langs(0)))
  posts(1)=new Posting(1,1, None,None,10,Some(testObject.langs(1)))
  posts(2)=new Posting(2,3, None,Some(0),-5,Some(testObject.langs(1)))
  posts(3)=new Posting(2,4, None,Some(0),10,Some(testObject.langs(1)))
  posts(4)=new Posting(2,5, None,Some(0),53,Some(testObject.langs(3)))
  posts(5)=new Posting(2,6, None,Some(1),70,Some(testObject.langs(4)))

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  @Test def `testObject can be instantiated`: Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  @Test def `testGrouping`: Unit = {
    val res=testObject.groupedPostings(sc.parallelize(posts))
    assert(res.count()>0, "Join failed")
    val pc=res.flatMap(a=>a._2.map(b=>(b._1.id,b._2.parentId))).collect().toArray
    pc.foreach(p=>
      assert(p._1==p._2.get, "Parent id ("+ p._1+ ") and child id (" + p._2.get + ") are different")
    )
  }

  @Test def `testscoredPostings`: Unit = {
    val res=testObject.scoredPostings(testObject.groupedPostings(sc.parallelize(posts)))
    res.map(r=>(r._1.id,r._2)).collect().map(q=>{
      val re=posts.filter(p=>p.parentId==Some(q._1))
      assert(re.length>0,"No answer for")
      val maxScore=re.maxBy(_.score).score
      assert(q._2==maxScore,"Highest score por question id "+q._1+" should be "+maxScore+" not "+q._2)
    })
  }


  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}

