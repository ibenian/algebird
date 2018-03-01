package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.{ Matchers, _ }
import scala.math.Equiv

class MinHasherTest extends CheckProperties {
  implicit val mhMonoid = new MinHasher32(0.5, 512)
  implicit val mhGen = Arbitrary {
    for (
      v <- Gen.choose(0, 10000)
    ) yield (mhMonoid.init(v))
  }

  property("MinHasher is a commutative monoid") {
    implicit val equiv: Equiv[MinHashSignature] = Equiv.by(_.bytes.toList)
    commutativeMonoidLawsEquiv[MinHashSignature]
  }
}

class MinHasherSpec extends WordSpec with Matchers {
  val r = new java.util.Random(123456789)

  def test[H](mh: MinHasher[H], similarity: Double, epsilon: Double): Double = {
    val (set1, set2) = randomSets(similarity)

    val exact = exactSimilarity(set1, set2)
    val sim = approxSimilarity(mh, set1, set2)
    val error: Double = math.abs(exact - sim)
    info(s"exact: $exact, sim: $sim, error: $error")
    assert(error < epsilon)
    error
  }

  def randomSets(similarity: Double) = {
    val s = 100000
    val uniqueFraction = if (similarity == 1.0) 0.0 else (1 - similarity) / (1 + similarity)
    val sharedFraction = 1 - uniqueFraction
    val unique1 = 1.to((s * uniqueFraction).toInt).map{ i => r.nextDouble }.toSet
    val unique2 = 1.to((s * uniqueFraction).toInt).map{ i => r.nextDouble }.toSet

    val shared = 1.to((s * sharedFraction).toInt).map{ i => r.nextDouble }.toSet
    (unique1 ++ shared, unique2 ++ shared)
  }

  def exactSimilarity[T](x: Set[T], y: Set[T]) = {
    (x & y).size.toDouble / (x ++ y).size
  }

  def approxSimilarity[T, H](mh: MinHasher[H], x: Set[T], y: Set[T]) = {
    val sig1 = x.map{ l => mh.init(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    val sig2 = y.map{ l => mh.init(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    val s = mh.similarity(sig1, sig2)
    println(s"similarity= ${s}")
    s
  }

  // "MinHasher32" should {
  //   "measure 0.5 similarity in 1024 bytes with < 0.1 error" in {
  //     test(new MinHasher32(0.5, 1024), 0.5, 0.1)
  //   }
  //   "measure 0.8 similarity in 1024 bytes with < 0.1 error" in {
  //     test(new MinHasher32(0.8, 1024), 0.8, 0.1)
  //   }
  //   "measure 1.0 similarity in 1024 bytes with < 0.01 error" in {
  //     test(new MinHasher32(1.0, 1024), 1.0, 0.01)
  //   }
  // }

  // "MurmurHash128" should {
  //   val h = new MurmurHash128(123)
  //   for {
  //     i <- 1 to 4000
  //   } {
  //     val (l1, l2) = h(i.toString)
  //     //val s = "(" + l1.toHexString + ", " + l2.toHexString + ")"
  //     //val s = "(x:" + l1.toString + ",y:" + l2.toString + ")"
  //     val s = "(xl:" + l1.toInt.toString + ",xh:" + (l1 >> 32).toInt.toString + ")"
  //     info(s)
  //   }

  //   // [info] MurmurHash128
  //   // [info] + ( ac2b6ac 7830176b, 20168331 f512c9a0)
  //   // [info] + ( 80c167c 391df6a8, 80c2b729 2f43ff75)
  //   // [info] + (f1b8f0b2 d6b4dcc7, d45851ec da26169d)
  //   // [info] + (59880125 2cac7274, 37711fc8 6967a6da)
  //   // [info] + (21bee7ea 7021e09e, d9ac9c6c 9a76eb5e)
  //   // [info] + (5a18dff6 67e52cba, 133202c1 f756a816)
  //   // [info] + (839830d8 2875034f, d9d3904b bc384261)
  //   // [info] + (2ba4ab65 aa284850, 57e2323f 3765e46d)
  //   // [info] + (dce37e00 3732341c, 5549d806 ab0593c7)
  //   // [info] + (75f842fe 39ca5135, 2686cb74 ebbc7557)
  // }

  "MinHasher32 init2" should {
    // info(new MinHasher32(3, 1).init("abc").bytes.mkString(", "))
    // info(new MinHasher32(3, 1).init2("abc").bytes.mkString(", "))

    val ntests = 100
    val avgError = 1.to(ntests).map(_ => test(new MinHasher32(0.5, 20 * 1024), 0.5, 0.1)).sum / ntests
    info(s"avgError: $avgError")

    // with init, 1000 samples
    // + avgError: 0.022553191489361697

    // with init2, 1000 samples
    // + avgError: 0.020851063829787225

    // with init, 10000 samples
    // + avgError: 0.03489361702127659

    // with init2, 10000 samples
    // + avgError: 0.02553191489361702

    // with init, 10000 samples, 20*1024 hashes
    // + avgError: 0.007070707070707077

    // with init, 10000 samples, 20*1024 hashes
    // + avgError: 0.003980986333927505

    // with init, 10000 samples, 1024 hashes
    // + avgError: 0.026510638297872334

    // with init2, 10000 samples, 1024 hashes
    // + avgError: 0.026170212765957438
  }
}
