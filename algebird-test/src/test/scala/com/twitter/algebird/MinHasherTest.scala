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
    val s = 10000
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
    val sig1 = x.map{ l => mh.init2(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    val sig2 = y.map{ l => mh.init2(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    val s = mh.similarity(sig1, sig2)
    println(s"similarity= ${s}")
    s
  }

  "MinHasher32" should {
    "measure 0.5 similarity in 1024 bytes with < 0.1 error" in {
      test(new MinHasher32(0.5, 1024), 0.5, 0.1)
    }
    "measure 0.8 similarity in 1024 bytes with < 0.1 error" in {
      test(new MinHasher32(0.8, 1024), 0.8, 0.1)
    }
    "measure 1.0 similarity in 1024 bytes with < 0.01 error" in {
      test(new MinHasher32(1.0, 1024), 1.0, 0.01)
    }
  }

  "MinHasher32 init2" should {
    info(new MinHasher32(3, 1).init("abc").bytes.mkString(", "))
    info(new MinHasher32(3, 1).init2("abc").bytes.mkString(", "))

    val avgError = 1.to(10).map(_ => test(new MinHasher32(0.5, 20 * 1024), 0.5, 0.1)).sum / 10
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

  }
}
