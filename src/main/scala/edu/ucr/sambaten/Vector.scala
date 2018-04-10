/**
 * Created by Jiahuan on 2018/01/31.
 */
package edu.ucr.sambaten

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import breeze.linalg.{DenseVector => BDV}
import breeze.stats.distributions.Rand

class CoordinateVector (
		_entries: RDD[TEntry],
		_shape: Coordinate)(
		implicit _sc: SparkContext) extends CoordinateTensor(_entries, _shape) {
	assert(dims == 1)
	val length = shape(0)

	def this(_entries: RDD[TEntry], len: Int)(implicit _sc: SparkContext) =
		this(_entries, Coordinate(len))

	def multiply(other: CoordinateVector): Double = {
		modeMultiply(0, other, 0).mapEntries(_.value).reduce(_+_)
	}

	def toBDV: BDV[Double] = {
		val vec = BDV.zeros[Double](shape(0))
		entries.collect().foreach { case TEntry(coord, value) =>
			vec(coord(0)) = value
		}
		vec
	}

	def normalize: (CoordinateVector, Double) = {
		val norm = scala.math.sqrt(mapEntries(e => e.value * e.value).reduce(_+_))
		(new CoordinateVector(mapEntries 
			{case TEntry(c, v) => new TEntry(c, v / norm) }, shape), norm)
	}

	override def persist = new CoordinateVector(Util.persist(entries), shape)

	override def cache = new CoordinateVector(Util.cache(entries), shape)
}

object CoordinateVector {
	def apply(vec: BDV[Double])(implicit sc: SparkContext): CoordinateVector = {
		val entries = for {
			i <- 0 until vec.length
		} yield TEntry(Coordinate(i), vec(i))
		new CoordinateVector(sc.parallelize(entries), vec.length)
	}

	def apply(tensor: CoordinateTensor): CoordinateVector = {
		new CoordinateVector(tensor.entries, tensor.shape)(tensor.sc)
	}
}

object VectorOps {
	def outerProd(v1: BDV[Double], v2: BDV[Double], v3: BDV[Double]): Double = {
		val len = v1.length
		assert(len == v2.length && len == v3.length)
		(0 until len).map(i => v1(i) * v2(i) * v3(i)).reduce(_+_)
	}

	def outerProd(vecs: BDV[Double]*): Double = {
		assert(vecs.length > 0)
		val len = vecs(0).length
		vecs.foreach { vec => assert(vec.length == len) }
		(0 until len).map { i => 
			var prod = 1.0
			vecs.foreach { vec => prod = prod * vec(i) }
			prod
		}.reduce(_+_)
	}

	def outerProd
			(v1: CoordinateVector, v2: CoordinateVector, v3: CoordinateVector): Double = {
		val mapFunc = (_: TEntry) match { case TEntry(coord, value) => (coord(0), value) }
		val combineFunc = (_: (Int, (Double, Double))) match { case (i, (val1, val2)) => (i, val1 * val2) }
		v1.mapEntries(mapFunc)
			.join(v2.mapEntries(mapFunc)).map(combineFunc)
			.join(v3.mapEntries(mapFunc)).map(combineFunc)
			.map(_._2).reduce(_+_)
	}

	def outerProd(vecs: CoordinateVector*)(implicit d: DummyImplicit): Double = {
		if (vecs.isEmpty) 0.0 else {
			val mapFunc = (_: TEntry) match { case TEntry(coord, value) => (coord(0), value) }
			val combineFunc = (_: (Int, (Double, Double))) match { case (i, (val1, val2)) => (i, val1 * val2) }
			vecs.drop(1).foldLeft(vecs(0).mapEntries(mapFunc)) { (acc, vec) =>
				acc.join(vec.mapEntries(mapFunc)).map(combineFunc)
			}.map(_._2).reduce(_+_)
		}
	}
}

class TestVector(implicit val sc: SparkContext) {
	val v1 = BDV(2.0,2,2,2)
	val v2 = BDV(0.5,0.5,0.5,0.5)
	val v3 = BDV(1.0,1,1,1)
	val v4 = BDV(1.0,2,3,4)

	def testOps: Unit = {
		assert(CoordinateVector(v1).toBDV == v1)
		assert(CoordinateVector(v1).multiply(CoordinateVector(v4)) == 20)
	}

	def testNorm: Unit = {
		val cv1 = CoordinateVector(v1)
		assert(cv1.toBDV == CoordinateVector(cv1.asInstanceOf[CoordinateTensor]).toBDV)
		assert(cv1.normalize._2 == 4 && cv1.normalize._1.toBDV == v2)
	}
	
	testOps
	testNorm
}