/**
 * Created by Jiahuan on 2018/04/09.
 */
package edu.ucr.sambaten

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, isClose => isValClose}

import scala.reflect.ClassTag

package object Util {
	type FactMat = Array[BDV[Double]]
	type IdxSeq = Seq[Seq[Int]]
	type IdxMap = Seq[Seq[Int]]
	private val EPSILON = 1e-7
	def isZero(v: Double) = isValClose(v, 0.0, EPSILON)
	def isClose(v1: Double, v2: Double) = isValClose(v1, v2, EPSILON)
	def toBDM(factMat: FactMat) = {
		val nRow = factMat.length;
		val nCol = if (!factMat.isEmpty) factMat(0).length else 0
		val newMat = BDM.zeros[Double](nRow, nCol)
		for (i <- 0 until nRow; j <- 0 until nCol) newMat(i, j) = factMat(i)(j)
		newMat
	}
	def persist[R: ClassTag](rdd: RDD[R]): RDD[R] = rdd.persist(StorageLevel.MEMORY_AND_DISK)
	def cache[R: ClassTag](rdd: RDD[R]): RDD[R] = rdd.cache
}