/**
 * Created by Jiahuan on 2018/02/18.
 */
package edu.ucr.sambaten

import scala.reflect.ClassTag

case class Coordinate(private val coord: IndexedSeq[Int]) extends Serializable {
  val dims: Int = coord.length

  def at(idx: Int): Int = coord(idx)

  def keep(idx: Int*): Coordinate = {
    coord.zipWithIndex.filter(pair => idx.contains(pair._2)).map(_._1)
  }

  def without(idx: Int*): Coordinate = {
    coord.zipWithIndex.filter(pair => !idx.contains(pair._2)).map(_._1)
  }

  def append(vals: Int*): Coordinate = coord ++ vals.toIndexedSeq

  def append(other: Coordinate): Coordinate = coord ++ other.coord

  def updated(idx: Int, elem: Int): Coordinate = coord.updated(idx, elem)

  def apply(idx: Int): Int = coord(idx)

  override def toString: String = {
    coord.foldLeft("(") { (acc, e) =>
      (acc, e) match {
        case ("(", e) => acc + e.toString
        case _ => acc + ", " + e.toString
      }
    } + ")"
  }

  def map[R: ClassTag](f: Int => R) = coord.map(f)

  def zipWithIndex = coord.zipWithIndex
}

object Coordinate {
  
  def apply(dims: Int*): Coordinate = new Coordinate(dims.toIndexedSeq)

  implicit def Seq2Coord(dims: Int*): Coordinate = Coordinate(dims: _*)

  implicit def IndexedSeq2Coord(idxSeq: IndexedSeq[Int]): Coordinate = new Coordinate(idxSeq)

  implicit def Tuple2Coord(tuple: Product): Coordinate = {
    val dims = tuple.productIterator.foldLeft(Nil: Seq[Int])((acc, v) =>
      acc :+ v.asInstanceOf[Int])
    Coordinate(dims: _*)
  }
}

class TestCoordinate() {
  val coord = Coordinate(3, 2, 1)
  assert(coord.dims == 3)
  assert(coord.at(1) == 2)
  assert(coord(1) == 2)
  assert(coord.append(0) == Coordinate(3,2,1,0))
  assert(coord.append(0).without(1,3) == Coordinate(3,1))
  assert(coord.keep(1) == Coordinate(2))
  assert(coord.updated(1, 200) == Coordinate(3,200,1))
}