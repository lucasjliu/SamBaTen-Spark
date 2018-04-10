/**
 * Created by Jiahuan on 2018/01/31.
 */
package edu.ucr.sambaten

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{
	CoordinateMatrix => SCM, BlockMatrix => SBM, MatrixEntry => SME}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, pinv}
import breeze.stats.distributions.Rand

class CoordinateMatrix (
		_entries: RDD[TEntry],
		_shape: Coordinate)(
		implicit _sc: SparkContext) extends CoordinateTensor(_entries, _shape) {
	assert(dims == 2)
	val nRow = shape(0)
	val nCol = shape(1)

	def this(_entries: RDD[TEntry], _nRow: Int, _nCol: Int)(implicit _sc: SparkContext) =
		this(_entries, Coordinate(_nRow, _nCol))

	// Dot product with another matrix
	def multiply(other: CoordinateMatrix): CoordinateMatrix = 
		CoordinateMatrix(modeMultiply(1, other, 0))

	def transpose: CoordinateMatrix = {
		val newEntries = mapEntries { case TEntry(coord, value) =>
				TEntry(Coordinate(coord(1), coord(0)), value) }
		new CoordinateMatrix(newEntries, nCol, nRow)
	}

	// Inner product with itself
	def gramian: CoordinateMatrix = transpose multiply this// X.t * X

	def toBDM: BDM[Double] = {
		val mat = BDM.zeros[Double](shape(0), shape(1))
		entries.collect().foreach { case TEntry(coord, value) =>
			mat(coord(0), coord(1)) = value
		}
		mat
	}

	// Pseudo inverse of the matrix
	def pseudoInv: CoordinateMatrix = CoordinateMatrix(pinv(toBDM))

	// Hadamard product with another matrix
	def hadamard(other: CoordinateMatrix): CoordinateMatrix =
		CoordinateMatrix(elemWiseOp(other, _*_))

	override def persist = new CoordinateMatrix(Util.persist(entries), shape)

	override def cache = new CoordinateMatrix(Util.cache(entries), shape)
}

object CoordinateMatrix {
	def apply(mat: BDM[Double])(implicit sc: SparkContext): CoordinateMatrix = {
		val entries = for { 
			i <- 0 until mat.rows
			j <- 0 until mat.cols
		} yield TEntry(Coordinate(i, j), mat(i, j))
		new CoordinateMatrix(sc.parallelize(entries), mat.rows, mat.cols)
	}

	def apply(tensor: CoordinateTensor): CoordinateMatrix = {
		new CoordinateMatrix(tensor.entries, tensor.shape)(tensor.sc)
	}

	def rand(nRow: Int, nCol: Int, distribution: Rand[Double] = Rand.uniform)(
			implicit sc: SparkContext): CoordinateMatrix = {
		CoordinateMatrix(BDM.rand[Double](nRow, nCol, distribution))
	}
}

class ColMatrix (
		val cols: Array[CoordinateVector],
		_entries: RDD[TEntry],
		_shape: Coordinate)(
		implicit _sc: SparkContext) extends CoordinateMatrix(_entries, _shape) {
	def normByCol: (ColMatrix, Array[Double]) = {
		val pairs = cols.map(_.normalize)
		val newCols = pairs.map(_._1).toArray
		val lambda = pairs.map(_._2).toArray
		require(lambda.forall(_ >= 0.0), lambda.mkString(","))
		(ColMatrix(newCols), lambda)
	}

	override def transpose: ColMatrix = ColMatrix(CoordinateMatrix(toBDM.t))

	override def gramian = CoordinateMatrix(this).gramian

	override def persist: ColMatrix =
		new ColMatrix(cols.map(v => v.persist), Util.persist(entries), shape)
}

object ColMatrix {
	def apply(cols: Array[CoordinateVector])(implicit sc: SparkContext): ColMatrix = {
		val nCol = cols.length
		val nRow = if (cols.isEmpty) 0 else cols(0).shape(0)
		val entries = (0 until nCol).foldLeft(sc.parallelize(Seq.empty[TEntry])) {
			(acc: RDD[TEntry], c: Int) => acc.union(cols(c).mapEntries {
				case TEntry(coord, value) => TEntry(coord.append(c), value)
			})
		}
		new ColMatrix(cols, entries, Coordinate(nRow, nCol))
	}

	def apply(mat: CoordinateMatrix): ColMatrix = {
		implicit val sc = mat.sc
		var cols = Array.fill[CoordinateVector](mat.shape(1))(
			new CoordinateVector(sc.parallelize(Seq.empty[TEntry]), 0)) // init by empty vector
		mat.mapEntries { case TEntry(coord, value) => 
			(coord(1), TEntry(coord.keep(0), value))
		}.collect // assume can fit in memory
		.groupBy(_._1).mapValues(_.map { case (_, e) => e })
		.foreach { case (colIdx: Int, colEntries: Array[TEntry]) =>
			cols(colIdx) = new CoordinateVector(sc.parallelize(colEntries.toSeq), mat.nRow)
		}
		new ColMatrix(cols, mat.entries, mat.shape)
	}

	def apply(mat: BDM[Double])(implicit sc: SparkContext): ColMatrix =
		ColMatrix(CoordinateMatrix(mat))

	def rand(nRow: Int, nCol: Int, distribution: Rand[Double] = Rand.uniform)(
			implicit sc: SparkContext): ColMatrix = 
		ColMatrix(CoordinateMatrix.rand(nRow, nCol, distribution))
}

class TestMatrix(implicit val sc: SparkContext) {
	private val logger = Logger.getLogger(getClass.getName)

	val m1 = BDM((1.0,2.0,3.0),(1.0,2.0,3.0),(1.0,2.0,3.0))
	val m2 = BDM((1.0,2.0,3.0),(4.0,5.0,6.0),(0.0,0.0,0.0))
	val cm1 = CoordinateMatrix(m1)
	val cm2 = CoordinateMatrix(m2)
	def testOps: Unit = {
		val bdm1 = cm1.toBDM
		val bdm2 = cm2.toBDM
		assert(cm1.gramian.toBDM == bdm1.t * bdm1)
		assert(cm2.gramian.toBDM == bdm2.t * bdm2)
		val rm1 = ColMatrix(cm2)
		assert(rm1.normByCol._2.length == rm1.nCol)
		assert(rm1.toBDM.t == rm1.transpose.toBDM)
	}

	def testConstruct: Unit = {
		assert(cm2.toBDM == m2)
		val rm2 = ColMatrix(cm2)
		assert(rm2.toBDM == m2)
		assert(ColMatrix(rm2.cols).toBDM == m2)
	}

	def toSCM(mat: CoordinateMatrix): SBM = {
		val blockSize = 1024
		val entries = mat.entries.map { case TEntry(coord, value) =>
			SME(coord(0).toLong, coord(1).toLong, value) }
		new SCM(entries, mat.nRow.toLong, mat.nCol.toLong).toBlockMatrix(blockSize, blockSize)
	}

	// Compare with Spark MLlib built-in matrix multiplication
	def stressTestMatrixMult(n: Int): Unit = {
		val cm1 = CoordinateMatrix.rand(n, n)
		val cm2 = CoordinateMatrix.rand(n, n)
		val bm1 = toSCM(cm1); val bm2 = toSCM(cm2)
		logger.error("matrix mult")
		(bm1 multiply bm2).blocks.collect
		logger.error("matrix mult done")
		logger.error("matrix mult")
		(cm1 multiply cm2).entries.collect
		logger.error("matrix mult done")

		logger.error("matrix mult")
		(cm1 multiply cm2).entries.collect
		logger.error("matrix mult done")
		logger.error("matrix mult")
		(bm1 multiply bm2).blocks.collect
		logger.error("matrix mult done")
	}
	
	testOps
	testConstruct
	//stressTestMatrixMult(100)
}