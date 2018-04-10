/**
 * Created by Jiahuan on 2018/03/10.
 */
package edu.ucr.sambaten

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM}
import breeze.stats.distributions.RandBasis
import breeze.optimize.linear.{KuhnMunkres => KM}

import scala.util.Random
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

import Util._

class SambaTensor(
		private val weights: Seq[BDV[Double]],
		_entries: RDD[TEntry],
		_shape: Coordinate)(
		implicit _sc: SparkContext) extends CoordinateTensor(_entries, _shape) {
	def append(other: CoordinateTensor, n: Int = dims - 1): SambaTensor = {
		val otherWeights = SambaTensor.initWeight(other)
		val newWeights = (0 until dims).map { dim =>
			if (dim == n) BDV(weights(dim).toArray ++ otherWeights(dim).toArray)
			else weights(dim) + otherWeights(dim)
		}
		val newTensor = modeAppend(other)
		new SambaTensor(newWeights, newTensor.entries, newTensor.shape)
	}

	def denseWeightedSampling(sampleShape: Coordinate)(
			implicit rand: Random=new Random): (CoordinateTensor, Seq[Seq[Int]]) = {
		val idxSeqs = (0 until dims).map(
			dim => SambaTensor.aResSampling(weights(dim), sampleShape(dim))).toSeq
		(SambaTensor.getSample(this, idxSeqs), idxSeqs)
	}

	override def persist = new SambaTensor(weights, Util.persist(entries), shape)

	override def cache = new SambaTensor(weights, Util.cache(entries), shape)
}

object SambaTensor {
	def apply(initTensor: CoordinateTensor)(implicit sc: SparkContext): SambaTensor = 
		new SambaTensor(initWeight(initTensor), initTensor.entries, initTensor.shape)
	
	def apply(entries: RDD[TEntry], shape: Coordinate)(implicit _sc: SparkContext): SambaTensor =
		apply(new CoordinateTensor(entries, shape))

	def initWeight(tensor: CoordinateTensor): Seq[BDV[Double]] = { //// sort at local?
		(0 until tensor.dims).map { dim =>
			BDV(tensor.mapEntries { case TEntry(coord, value) => (coord(dim), value)}
			.groupByKey.sortByKey()
			.map(_._2).map(_.reduce(_+_))
			.collect)
		}.toIndexedSeq
	}

	def aResSampling(weights: BDV[Double], sampleSize: Int)(
			implicit rand: Random=new Random): Seq[Int] = {
		val minHeap = PriorityQueue[(Double,Int)]()(Ordering.by(-_._1))
		weights.toArray.zipWithIndex.foreach { case (weight, idx) =>
			val score = math.pow(rand.nextDouble, 1.0 / weight)
			if (minHeap.size < sampleSize || minHeap.head._1 < score)
				minHeap.enqueue((score, idx))
			if (minHeap.size > sampleSize)
				minHeap.dequeue
		}
		minHeap.toSeq.map(_._2).toIndexedSeq.sorted
	}

	def getSample(tensor: CoordinateTensor,
			idxSeqs: Seq[Seq[Int]])(
			implicit sc: SparkContext): CoordinateTensor = {
		val sampleShape = Coordinate(idxSeqs.map(_.length): _*)
		val idxSets = idxSeqs.map(_.toSet)
		val idxMaps = idxSeqs.map(_.zipWithIndex.map { 
			case (oldIdx, newIdx) => oldIdx -> newIdx }.toMap)
		val broadIdxSets = sc.broadcast(idxSets)
		val broadIdxMaps = sc.broadcast(idxMaps)
		val sampleEntires = tensor.entries.filter(_.coord.zipWithIndex.map {
			case (idx, dim) => broadIdxSets.value(dim)(idx) }.reduce(_&&_)
		).map { case TEntry(coord, value) =>
			val newCoord = new Coordinate(coord.zipWithIndex.map { 
				case (idx, dim) => broadIdxMaps.value(dim)(idx) })
			new TEntry(newCoord, value)
		}
		new CoordinateTensor(sampleEntires, sampleShape)
	}
}

class SambatenModel(
		val _factMats: Array[FactMat],
		val _lambda: BDV[Double]) extends CPDecompModel(_factMats, _lambda) {
	def this(initModel: CPDecompModel) = this(initModel.factMats, initModel.lambda)

	def getSample(idxSeqs: Seq[Seq[Int]]): SambatenModel = {
		assert(idxSeqs.length == factMats.length)
		val newFactMats = factMats.zipWithIndex.map { case (factMat, dim) =>
			idxSeqs(dim).map(idx => factMat(idx)).toArray }
		new SambatenModel(newFactMats, lambda)
	}

	def getSample(lens: Seq[Int])(implicit d: DummyImplicit): SambatenModel = 
		getSample(lens.map(len => (0 until len)))

	def updateWith(newFactMats: Seq[BDM[Double]], idxMaps: Seq[Seq[Int]],
			incDim: Int, incSize: Int, repetitions: Int): Unit = {
		newFactMats.zipWithIndex.map { case (newFactMat, dim) =>
			if (dim == incDim) {
				assert(newFactMats(dim).rows == idxMaps(dim).length + incSize)
				val beg = factMats(dim).length - incSize
				val incBeg = newFactMat.rows - incSize
				val rank = newFactMat.cols
				// average out and add to new slice
				for (r <- 0 until incSize; c <- 0 until rank)
					factMats(dim)(beg+r)(c) += newFactMat(incBeg+r, c) / repetitions
			} else assert(newFactMats(dim).rows == idxMaps(dim).length)
			updateDimWith(dim, newFactMat, idxMaps(dim))
		}
	}

	private def updateDimWith(dim: Int, newFactMat: BDM[Double], idxMap: Seq[Int]): Unit = {
		assert(newFactMat.cols == rank)
		for { r <- 0 until idxMap.length; c <- 0 until newFactMat.cols
			if (Util.isZero(factMats(dim)(idxMap(r))(c)))
		} factMats(dim)(idxMap(r))(c) = newFactMat(r, c)
	}

	def append(dim: Int, newFactMat: BDM[Double]): Unit = {
		val buf = ArrayBuffer(factMats(dim): _*)
		for (r <- 0 until newFactMat.rows) buf += newFactMat(r, ::).t
		factMats(dim) = buf.toArray
	}
}

class Sambaten(
		initTensor: CoordinateTensor,
		initModel: CPDecompModel,
		private val incDim: Int,
		private val samplingFactor: Int,
		private val repetitions: Int = 1,
		rank: Int = 5,
		maxIter: Int = 500,
		tol: Double = 1e-4)(
		implicit val sc: SparkContext) {
	private val logger = Logger.getLogger(getClass.getName)
	var tensor = SambaTensor(initTensor).persist
	var model = new SambatenModel(initModel)
	val als = new CPALS(rank, maxIter, tol)

	def receive(newSlice: CoordinateTensor)(
			implicit rand: Random=new Random): CPDecompModel = {
		assert(tensor.shape.without(incDim) == newSlice.shape.without(incDim))
		val batchSize = newSlice.shape(incDim)
		val dims = tensor.dims
		val sampleShape = (0 until dims).map(tensor.shape(_) / samplingFactor)
		val newTensorShape = sampleShape.updated(incDim, sampleShape(incDim) + batchSize)
		model.append(incDim, BDM.zeros[Double](batchSize, rank))
		for (rep <- 0 until repetitions) {
			val (sampleTensor, sampleIdxs) = tensor.denseWeightedSampling(sampleShape)
			val newTensor = sampleTensor.modeAppend(SambaTensor.getSample(
				newSlice, sampleIdxs.updated(incDim, (0 until batchSize))), incDim)
			val newModel = new SambatenModel(als.run(newTensor.persist))
			val refs = model.getSample(sampleIdxs).factMats.map(toBDM(_))
			val newFactMats = rearrangeRank(newModel.factMats.map(toBDM(_)).toSeq, refs(0))
			(0 until dims).foreach(dim => renorm(newFactMats(dim), refs(dim)) )
			model.updateWith(newFactMats, sampleIdxs, incDim, batchSize, repetitions)
		}
		tensor = tensor.append(newSlice, incDim).persist
		model.asInstanceOf[CPDecompModel]
	}

	private def rearrangeRank(factMats: Seq[BDM[Double]], refA: BDM[Double]): Seq[BDM[Double]] = {
		assert(!factMats.isEmpty)
		val A = CoordinateMatrix(factMats(0))
		val ref = ColMatrix(refA).normByCol._1
		require(A.shape == ref.shape, "A.shape:" + A.shape.toString + " ref.shape:" + ref.shape.toString)
		//logger.error("Before re-arrange: ")
		//println(factMats(0))
		//logger.error("ref: ")
		//println(ref.toBDM)
		val rank = A.nCol
		val similarity = (A.transpose multiply ref).toBDM
		for (r <- 0 until similarity.rows; c <- 0 until similarity.cols)
			similarity(r, c) = math.abs(similarity(r, c))
		val cost = similarity * -1.0 :+ 1.0
		val rankMap = KM.extractMatching(
			(0 until rank).map(r => cost(r, ::).t.toArray.toSeq).toSeq)._1
		assert(rankMap.distinct.length == rank)
		val rearrangedFactMats = factMats.map { mat =>
			val nRow = mat.rows
			val newMat = BDM.zeros[Double](nRow, rank)
			for (i <- 0 until nRow; r <- 0 until rank)
				newMat(i, rankMap(r)) = mat(i, r)
			newMat
		}
		//logger.error("After re-arrange: ")
		//println(rearrangedFactMats(0))
		rearrangedFactMats
	}

	private def renorm(C: BDM[Double], ref: BDM[Double]): Unit = {
		val sampleSize = ref.rows
		assert(rank == C.cols)
		// C.rows >= ref.rows
		val lambdaRef = (0 until rank).map { c =>
			math.sqrt((0 until sampleSize).map(r => ref(r,c)*ref(r,c)).reduce(_+_)) }
		val lambdaC = (0 until rank).map { c =>
			math.sqrt((0 until sampleSize).map(r => C(r,c)*C(r,c)).reduce(_+_)) }
		val sign = (v: Double) => { if (v >= 0) 1.0 else -1.0 }
		val signC = (0 until rank).map { c =>
			sign((0 until sampleSize).map(r => sign(ref(r, c)) / sign(C(r, c))).reduce(_+_)) }
		for (r <- 0 until C.rows; c <- 0 until rank)
			C(r, c) *= (lambdaRef(c) / lambdaC(c) * signC(c))
	}
}

class SyntheticIncDataset(fullModel: SambatenModel, incDim: Int) {
	var incItr = 0
	val shape = fullModel.shape
	def genNewData(batchSize: Int)(implicit sc: SparkContext): Option[CoordinateTensor] = {
		if (incItr >= shape(incDim)) None
		else {
			val end = math.min(incItr + batchSize, shape(incDim))
			val idxSeqs = shape.map(len => (0 until len)).updated(incDim, (incItr until end))
			incItr += batchSize
			Some(fullModel.getSample(idxSeqs).reconstruct.persist)
		}
	}
}

class TestSambaten(implicit val sc: SparkContext) {
	private val logger = Logger.getLogger(getClass.getName)
	def testSample = {
		val fullModel = new SambatenModel(TestCPALS.rand(Coordinate(5,5,5), 5))
		val fullTensor = SambaTensor(fullModel.reconstruct.persist)
		val (sampleTensor, idxSeqs) = fullTensor.denseWeightedSampling(Coordinate(3,3,3))
		assert( isZero(fullModel.getSample(idxSeqs).test(sampleTensor)) )
	}

	def stressTest = {
		val seed = 0 ////not working yet
		implicit val basis = RandBasis.withSeed(seed)
		implicit val rand = new Random(seed)
		val rank = 5; val tol=1e-4; val rep = 1
		val batchSize = 20
		val I = 100; val J = 100; val K = 100

		val fullModel = new SambatenModel(TestCPALS.rand(Coordinate(I,J,K), rank))
		val dataset = new SyntheticIncDataset(fullModel, 2)
		var slice = dataset.genNewData(batchSize)
		val als = new CPALS(rank=rank, tol=tol)
	
		val sambaten = new Sambaten(SambaTensor(slice.get),
			new SambatenModel(als.run(slice.get)),
			incDim=2, repetitions=rep, samplingFactor=2, rank=rank, tol=tol)
	
		slice = dataset.genNewData(batchSize)
		while (!slice.isEmpty) {
			logger.error("receive tensor " + slice.get.shape.toString)
			val model = sambaten.receive(slice.get)
			val error = model.test(sambaten.tensor)
			logger.error(s"curr relative error: $error")
			slice = dataset.genNewData(batchSize)
		}
		val fullTensor = fullModel.reconstruct.persist
		val error = als.run(fullTensor).test(fullTensor)
		logger.error(s"CPALS relative error: $error")
	}

	testSample
	//for (i <- 0 until 10) stressTest
}