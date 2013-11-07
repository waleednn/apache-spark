package catalyst
package trees

import catalyst.types.IntegerType
import expressions._

import org.scalatest.{FunSuite}

class TransformSuite extends FunSuite {

  test("top node changed") {
    val after = Literal(1) transform { case Literal(1, _) => Literal(2) }
    assert(after === Literal(2,IntegerType))
  }

  test("one child changed") {
    val before = Add(Literal(1), Literal(2))
    val after = before transform { case Literal(2, _) => Literal(1) }

    assert(after === Add(Literal(1), Literal(1)))
  }
}