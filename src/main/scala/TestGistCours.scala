import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TestGistCours extends App {
  println("Hello")

  val a: Array[Int] = Array(1,2,3,4,5,6,7,8)

  //Démarrer Spark
  val conf = new SparkConf().setAppName("Test en classe").setMaster("local[4]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  //Travailler sur nos données avec Spark
  //#1, il faut charger les données a l'intérieur du cluster

  val rdd = sc.makeRDD( a) //"Met la collection sur le cluster"
  val aa = rdd.map(  e => e * 1) //J'applique sur une fonction sur chaque élément


  println("Partitions avant le map qui transforme le RDD en PairRDD")

  val ff :RDD[String] = aa.mapPartitionsWithIndex((id, iterator) => {  //J'applique une fonction sur chaque partition
    var output = ""
    output += s" Partition #$id="
    iterator.foreach( e => output += e + " ")

    //Array, List, Map, HashMap  1,2,3,4,5  Iterable
    Iterator(output)
  })

  val resultat3: Array[String] = ff.collect()
  resultat3.foreach( println)


  /*
    On va transformer la valeur a, en une collection qui repète la valeur a fois.
    4 -> 4,4,4,4
    4 -> (1,1) (2,1) (3,1) (4,1) flatMap
    Ça va nous permettre de dénombrer les choses après.
    to/until
    Quand vous utilisez le to, vous allez égaliser la valeur dans votre comparaison <=4
    Quand vous utilisez le until, vous allez arreter juste un peu avant  <4
   */

  val cc: RDD[(Int, Int)] = aa.flatMap(e => {
    val retour = new ArrayBuffer[Tuple2[Int, Int]]()
    for (i <-1 to e)  {
      retour += Tuple2(i,1)
    }
    retour
  } ) //

  //Je vais calculer la somme intermédiaire de tout
  //Les reducers qui vont s'occuper de la même clé avec le shuffle. ON vvait un reducer qui s'occupait de (/lapin, 3h)

  // 4 -> (4,1) (4,1) (4,1) (4,1)
  //Reducebykey recoit 1,1,      1,1
  // a = 1  b=1
  //APrès un appel de reduceByKey il reste
  // a = 2  b = 1      restant : 1
  //Après un appel
  //  a=3   b =1

  println("Avant le shuffle")

  val ee: RDD[String] = cc.mapPartitionsWithIndex((id, iterator) => {  //J'applique une fonction sur chaque partition
    var output = ""
    output += s" Partition #$id="
    iterator.foreach( e => output += e + " ")

    //Array, List, Map, HashMap  1,2,3,4,5  Iterable
    Iterator(output)
  })

  val resultat: Array[String] = ee.collect()
  resultat.foreach( println)


  // a=4
  val dd = cc.reduceByKey( (a,b) => a+b)
  // dd.collect().foreach( println)

  println("Apres le shuffle")

  val bb: RDD[String] = dd.mapPartitionsWithIndex((id, iterator) => {  //J'applique une fonction sur chaque partition
    var output = ""
    output += s" Partition #$id="
    iterator.foreach( e => output += e + " ")

    //Array, List, Map, HashMap  1,2,3,4,5  Iterable
    Iterator(output)
  })

  val resultat2: Array[String] = bb.collect()
  resultat2.foreach( println)


  //  val bb: RDD[String] = aa.mapPartitionsWithIndex((id, iterator) => {  //J'applique une fonction sur chaque partition
  //      var output = ""
  //    output += s" Partition #$id="
  //      iterator.foreach( e => output += e + " ")
  //
  //    //Array, List, Map, HashMap  1,2,3,4,5  Iterable
  //      Iterator(output)
  //  })
  //
  //  val resultat: Array[String] = bb.collect()
  //  resultat.foreach( println)


  // p1 [1,2,3,4 ]      p2 [ 5,6,7,8]
  //RDD = Resilient Distributed Dataset.  Le RDD est un tableau distribué.

}
