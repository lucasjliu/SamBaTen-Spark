/**
 * Created by Jiahuan on 2018/02/18.
 */
package edu.ucr.sambaten

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.log4j.Logger
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, isClose => isValClose}
import breeze.stats.distributions.{RandBasis, Gaussian}

import Util._

/**
 * Model representing the result of CP-ALS
 *
 * @param factMats 	Factor matriecs. Stored locally so that they are able to broadcast
 										when testing a sparse tensor
 * @param lambda 		Lambda of normalization by (rank) column.
 */
class CPDecompModel(val factMats: Array[FactMat], val lambda: BDV[Double]) {
	val dims = factMats.length
	val rank = lambda.length
	assert(factMats(0)(0).length == rank)
	val shape = new Coordinate(factMats.map(_.length).toIndexedSeq)

	// Relative error
	def test(testingTensor: CoordinateTensor)(implicit sc: SparkContext): Double = {
		val broadFactMats = sc.broadcast(factMats)
		val broadLambda = sc.broadcast(lambda)
		val squaredErrorSum = testingTensor.mapEntries { case TEntry(coord, value) =>
			math.pow(CPDecompModel.eval(coord, broadFactMats.value, broadLambda.value) - value, 2)
		}.reduce(_+_)
		math.sqrt(squaredErrorSum) / testingTensor.norm
	}

	// Evaluate a prediction given the coordinate
	def eval(coord: Coordinate): Double = CPDecompModel.eval(coord, factMats, lambda)

	// Reconstruct by resulted model and return the full tensor
	def reconstruct(implicit sc: SparkContext): CoordinateTensor = {
		val broadFactMats = sc.broadcast(factMats)
		val broadLambda = sc.broadcast(lambda)
		val emptyCoord = sc.parallelize(Seq(Coordinate()))
		val idxRdds = factMats.map(factMat => sc.parallelize((0 until factMat.length).toSeq))
		// construct all possible coordinates of the full tensor,
		//	 then compute their values using broadcast factors
		val entries = idxRdds.foldLeft(emptyCoord) { (acc, idxRdd) =>
			acc.cartesian(idxRdd).map { case (coord, i) => coord.append(i) }
		}.map { coord => new TEntry(coord, 
			CPDecompModel.eval(coord, broadFactMats.value, broadLambda.value)) }
		new CoordinateTensor(entries, shape)
	}
}

object CPDecompModel {
	def apply(factMats: FactMat*): CPDecompModel = {
		val rank = factMats(0)(0).length
		new CPDecompModel(factMats.toArray, BDV.ones[Double](rank))
	}

	def apply(factMats: ColMatrix*)(implicit d: DummyImplicit): CPDecompModel =
		apply(factMats.toArray, BDV.ones[Double](factMats(0).nCol))

	def apply(factMats: Array[ColMatrix], lambda: BDV[Double]): CPDecompModel =
		new CPDecompModel(factMats.map(colMat => collectByCol(colMat)).toArray, lambda)

	def eval(coord: Coordinate, mats: Array[FactMat], lambda: BDV[Double]): Double = {
		val factVecs = (0 until coord.dims).map { i => mats(i)(coord(i)) }
		VectorOps.outerProd((lambda +: factVecs): _*)
	}

	def collectByCol(colMat: ColMatrix): FactMat = 
		colMat.transpose.cols.map(_.toBDV)
}
  
object LinalgUtil {
	// Initialize a factor matrix for CP-ALS training
	def initMat(nRow: Int, nCol: Int)(implicit b: RandBasis): BDM[Double] =
		BDM.rand[Double](nRow, nCol, Gaussian(0.0, 1.0))

	def isClose(tensor1: CoordinateTensor, tensor2: CoordinateTensor, tol: Double = 1e-6): Boolean = {
		assert(tensor1.shape == tensor2.shape)
		(tensor1-tensor2).forall(value => isValClose(value, 0.0, tol))
	}
}

/**
 * CP (PARAFAC) decomposition by alternating least squares (ALS)
 *
 * @param rank 			Rank for the tensor.
 * @param maxIter 	Maximum number of iterations.
 * @param tol 			Tolerance of error rate.
 */
class CPALS(
		private var rank: Int = 10,
		private var maxIter: Int = 500,
		private var tol: Double = 1e-4)(
		implicit val sc: SparkContext) {
	private val logger = Logger.getLogger(getClass.getName)

	def setAttr(r: Int, mi: Int, t: Double): this.type = {
    require(r > 0, s"Rank of the tenor must be positive but got $r")
    require(mi >= 0, s"Number of iterations must be nonnegative but got $mi")
    require(t >= 0, s"Tolerance must be nonnegative but got $t")
    rank = r; maxIter = mi; tol = t
    this
  }

	def run(tensor: CoordinateTensor)(
			implicit basis: RandBasis=RandBasis.withSeed(6)): CPDecompModel = {
		val shape = tensor.shape
		logger.error(s"CPALS on tensor$shape")
		val a = 0; val b = 1; val c = 2
		var factMats: Array[ColMatrix] = null
		var lambda: BDV[Double] = null
		var restart = true
		while (restart) { // in case of overflow, should not happen in most cases
			restart = false
			// initialize factors
			factMats = shape.map(len => 
				ColMatrix(LinalgUtil.initMat(len, rank)).persist).toArray
			lambda = BDV.ones[Double](rank)
			var prevError = Double.PositiveInfinity; var currError = 0.0
			var iter = 0
			while (!restart && iter < maxIter && 
					!isValClose(prevError, currError, tol)) {
				// update factor matrices, normalize them, and get lambda
				updateIteration(tensor, factMats, a, b, c)
				updateIteration(tensor, factMats, b, a, c)
				lambda = 
				updateIteration(tensor, factMats, c, a, b)
				// evaluate current training error
				prevError = currError
				currError = CPDecompModel(factMats, lambda).test(tensor)
				logger.error(s"$iter, $currError")
				iter += 1
				if (lambda(0).isNaN || lambda(0).isInfinity) restart = true
			}
		}
		CPDecompModel(factMats, lambda)
	}

	private def updateIteration(tensor: CoordinateTensor,
			factMats: Array[ColMatrix], a: Int, b: Int, c: Int): BDV[Double] = {
		val A = mttkrp(tensor, factMats, a) multiply
			(factMats(b).gramian hadamard factMats(c).gramian).pseudoInv.persist
		val (normA, lambda) = ColMatrix(A).normByCol
		factMats(a) = normA.persist
		new BDV(lambda)
	}

	// Matricized tensor times Khatri-Rao product
	// Each column is a rank of factors. Factor matrices are stored as columns so that 
	// 	the result matrix is computed a rank at a time. See HATEN2-PARAFAC-Naive for details.
	private def mttkrp(tensor: CoordinateTensor, 
			factMats: Array[ColMatrix], n: Int): ColMatrix = {
		assert(n < tensor.dims && rank == factMats(n).nCol)
		ColMatrix((0 until rank).map(r => {
			val res = (factMats.length - 1 to 0 by -1).foldLeft(tensor) { (acc, i) => 
				if (i == n) acc else acc.modeMultiplyVec(i, factMats(i).cols(r)) }
			CoordinateVector(res)
		}).toArray)
	}
}

class TestCPALS(implicit val sc: SparkContext) {
	private val logger = Logger.getLogger(getClass.getName)

	def simpleTest = {
		val factMats = Array(
			Array(BDV(2.0,2.0), BDV(3.0,3.0), BDV(4.0,4.0)),
			Array(BDV(1.0,1.0), BDV(2.0,2.0)),
			Array(BDV(5.0,5.0), BDV(4.0,4.0), BDV(3.0,3.0)))
		val lambda = BDV(5.0,5.0)
		val model = new CPDecompModel(factMats, lambda)

		assert(model.eval(Coordinate(0,0,0)) == 100.0)
		assert(model.eval(Coordinate(1,0,0)) == 150.0)
		assert(model.eval(Coordinate(2,1,2)) == 240.0)

		assert( CoordinateVector(CoordinateVector(BDV(1.0,0.0)) - CoordinateVector(BDV(0.0,1.0)))
			.toBDV == BDV(1.0,-1.0) )

		val reconsTensor = model.reconstruct.persist
		val dec = 3.0
		val testEntries = reconsTensor.mapEntries { entry =>
			if (entry.coord == Coordinate(0,0,0))
				new TEntry(entry.coord, entry.value - dec) else entry
		}
		val testTensor = new CoordinateTensor(testEntries, reconsTensor.shape).persist
		assert(Util.isClose(model.test(testTensor), math.sqrt(dec*dec)/testTensor.norm))

		stressTest(Coordinate(3,3,3), 2, 1)
	}

	/** Stress test on randomly generated dense tensor. Tensors are reconstructed 
	*		by randomly generated factor matrices. For each restart a model is trained
	*		and tested using the same input tensor.
  *
  * @param shape		Dimensions of the tensors
  * @param rank			Rank for both input and result tensors
  * @param restart	Number of restarts
  */
	def stressTest(shape: Coordinate, rank: Int, restart: Int): Unit = {
		val seed = RandBasis.withSeed(0)
		for (i <- 0 until restart) {
			val model = TestCPALS.rand(shape, rank)
			val label = model.reconstruct.persist
			logger.error("training")
			val t0 = System.nanoTime()
			val genModel = (new CPALS(rank=rank,tol=0.002)).run(label)
			val t1 = System.nanoTime
			val etime = (t1-t0)/1e9
			val error = genModel.test(label)
			logger.error(s"relative error: $error, time cost: $etime")
		}
	}

	simpleTest
	//stressTest(Coordinate(100,100,100), 5, 10)
}

object TestCPALS {
	// Randomly generate a CP model in certain shape and rank, in order to generate
	// a random tensor
	def rand(shape: Coordinate, rank: Int)(
			implicit basis: RandBasis=RandBasis.withSeed(0)): CPDecompModel = {
		val factMats = shape.map { len =>
			(0 until len).map { _ => BDV.rand[Double](rank) }.toArray
		}
		CPDecompModel(factMats: _*)
	}
}