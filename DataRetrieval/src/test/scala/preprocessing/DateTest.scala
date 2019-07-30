package preprocessing

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import preprocessing.IndexFile.Date


@RunWith(classOf[JUnitRunner])
class DateTest extends FunSuite {
  val smallestDate = Date("20181011200014")
  val mediumDate = Date("20181011200015")
  val biggestDate = Date("20181011200016")

  test("== should return True") {
    assert(smallestDate === smallestDate)
  }
  test("== should return False") {
    assert(!(smallestDate === mediumDate))
  }

  test("> should return True") {
    assert(biggestDate > mediumDate)
  }
  test("> should return False") {
    assert(!(smallestDate > biggestDate))
  }
  test("> with same dates, should return False") {
    assert(!(biggestDate > biggestDate))
  }

  test("< should return True") {
    assert(mediumDate < biggestDate)
  }
  test("< should return False") {
    assert(!(biggestDate < smallestDate))
  }
  test("< with same dates, should return False") {
    assert(!(biggestDate < biggestDate))
  }

  test("<= with same dates, should return True") {
    assert(biggestDate <= biggestDate)
  }
  test("<= should return True") {
    assert(mediumDate <= biggestDate)
  }
  test("<= should return False") {
    assert(!(biggestDate <= smallestDate))
  }


  test(">= with same dates, should return True") {
    assert(smallestDate >= smallestDate)
  }
  test(">= should return True") {
    assert(biggestDate >= smallestDate)
  }
  test(">= should return False") {
    assert(!(smallestDate >= mediumDate))
  }

}

