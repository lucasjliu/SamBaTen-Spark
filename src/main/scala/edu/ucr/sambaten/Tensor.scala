/**
 * Created by Jiahuan on 2018/02/18.
 */
package edu.ucr.sambaten

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.log4j.Logger

import scala.reflect.ClassTag

case class TEntry(val coord: Coordinate, val value: Double) {
  override def toString: String = s"($coord, $value)"
}

/**
 * Representation of tensor in coordinate format.
 *
 * @param entries 	Tensor entries in distributed storage.
 * @param shape 		Dimensions of the tensor.
 * @param nnz 			Number of non-zero entries.
 */
class CoordinateTensor (
    val entries: RDD[TEntry],
    final val shape: Coordinate,
    private var nnz: Int = -1)(
    implicit val sc: SparkContext) {
  private val logger = Logger.getLogger(getClass.getName)
  final val dims: Int = shape.dims

  def this(shape: Coordinate)(implicit sc: SparkContext) = 
    this(sc.parallelize(Seq.empty[TEntry]), shape)

  def mapEntries[R: ClassTag](f: TEntry => R): RDD[R] = entries.mapPartitions(_.map(f))//entries.map(f)

  def map(f: TEntry => TEntry) = new CoordinateTensor(entries.map(f), shape)

  def tuples = mapEntries { entry => (entry.coord, entry.value) }

  def persist = new CoordinateTensor(Util.persist(entries), shape)

  def cache = new CoordinateTensor(Util.persist(entries), shape)

  // Append with another tensor on a certain mode
  def modeAppend(other: CoordinateTensor, n: Int = dims - 1): CoordinateTensor = {
    assert(shape.without(n) == other.shape.without(n))
    val len = shape(n)
    // Increment the coordinate on mode n
    val shiftedEntries = other.entries.map { case TEntry(coord, value) =>
      new TEntry(coord.updated(n, coord(n) + len), value) }
    new CoordinateTensor(entries.union(shiftedEntries), 
      shape.updated(n, shape(n) + other.shape(n)),
      numNonZero + other.numNonZero)
  }

  /** General mode multiplication by shuffle join
  *
  * @param m			Mode of this tensor
  * @param other	The other tensor
  * @param n			Mode of the other tensor
  */
  def modeMultiply(m: Int, other: CoordinateTensor, n: Int): CoordinateTensor = {
    assert(shape(m) == other.shape(n))
    val leftShuffle = mapEntries { entry => 
      (entry.coord(m), (entry.coord.without(m), entry.value))
    }
    val rightShuffle = other.mapEntries { entry => 
      (entry.coord(n), (entry.coord.without(n), entry.value))
    }
    val newEntries = leftShuffle.join(rightShuffle).map {
      case (_, ((c1, v1), (c2, v2))) => ((c1, c2), v1 * v2)
    }.reduceByKey(_+_).map {
      case ((c1, c2), sum) => TEntry(c1.append(c2), sum) }
    val newShape = shape.without(m).append(other.shape.without(n))
    new CoordinateTensor(newEntries, newShape)
  }

  // Mode multiplication on a small tensor, optimized by map-side join
  def modeMultiplyVec(m: Int, vec: CoordinateVector): CoordinateTensor = {
    assert(shape(m) == vec.length)
    val broadVec = sc.broadcast(vec.toBDV)
    val newEntries = mapEntries { case TEntry(coord, value) =>
      (coord.without(m), value * broadVec.value(coord(m)))
    }.reduceByKey(_+_).map { case (coord, value) => TEntry(coord, value) }
    val newShape = shape.without(m)
    new CoordinateTensor(newEntries, newShape)
  }

  def numNonZero: Int = {
    if (nnz == -1) nnz = entries.count().toInt
    nnz
  }

  def elemWiseOp(other: CoordinateTensor, op: (Double, Double)=>Double): CoordinateTensor = {
    assert(shape == other.shape)
    val newEntries = tuples.join(other.tuples).map {
      case (coord, (v1, v2)) => new TEntry(coord, op(v1, v2) ) }
    new CoordinateTensor(newEntries, shape)
  }

  def elemWiseOpFull(other: CoordinateTensor, op: (Double, Double)=>Double): CoordinateTensor = {
    assert(shape == other.shape)
    val get = (opt: Option[Double]) => opt match {
      case Some(v) => v
      case None => 0.0
    }
    val newEntries = tuples.fullOuterJoin(other.tuples).map {
      case (coord, (v1, v2)) => new TEntry(coord, op(get(v1), get(v2)) ) }
    new CoordinateTensor(newEntries, shape)
  }

  def -(other: CoordinateTensor): CoordinateTensor = {
    elemWiseOpFull(other, _-_)
  }

  def forall(condition: Double => Boolean): Boolean = 
    entries.aggregate(true)((acc, e) => acc && condition(e.value), _&&_)

  def norm: Double =
    math.sqrt(mapEntries(e => e.value * e.value).reduce(_+_))

  override def toString = {
    s"CoordinateTensor$shape: " + 
    entries.collect().foldLeft("(") { (acc, entry) => 
      (acc, entry) match {
        case ("(", entry) => acc + entry.toString
        case _ => acc + ", "  +entry.toString
      }
    } + ")"
  }
}