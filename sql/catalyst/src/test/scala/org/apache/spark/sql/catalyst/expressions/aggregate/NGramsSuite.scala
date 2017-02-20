package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast, CreateArray, ExpressionEvalHelper, Literal}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by gcz on 17-2-20.
  */

case class MyBoundReference(size: Int, dataType: DataType, nullable: Boolean)
  extends LeafExpression {

  override def toString: String = s"input[$size, ${dataType.simpleString}, $nullable]"

  // Use special getter for primitive types (for UnsafeRow)
  override def eval(input: InternalRow): Any = {
    if (input.isNullAt(size)) {
      null
    } else {
      dataType match {
        case StringType => new GenericArrayData((0 until input.numFields).map(input.getUTF8String(_)).toArray)
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val value = ctx.getValue(ctx.INPUT_ROW, dataType, size.toString)
    if (ctx.currentVars != null && ctx.currentVars(size) != null) {
      val oev = ctx.currentVars(size)
      ev.isNull = oev.isNull
      ev.value = oev.value
      val code = oev.code
      oev.code = ""
      ev.copy(code = code)
    } else if (nullable) {
      ev.copy(code = s"""
        boolean ${ev.isNull} = ${ctx.INPUT_ROW}.isNullAt($size);
        $javaType ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(dataType)} : ($value);""")
    } else {
      ev.copy(code = s"""$javaType ${ev.value} = $value;""", isNull = "false")
    }
  }
}


class NGramsSuite extends SparkFunSuite with ExpressionEvalHelper{

  test("NGrams") {
    val abc = UTF8String.fromString("abc")
    val bcd = UTF8String.fromString("bcd")

    val pattern1 = List[UTF8String](abc, abc, bcd, abc, bcd)
    val pattern2 = List[UTF8String](bcd, abc, abc, abc, abc, bcd)

    var dataList1 = List[List[UTF8String]]()
    (0 until 100).foreach {
      _ => dataList1 = pattern1 :: dataList1
    }
    var data1 = dataList1.toArray

    var dataList2 = List[List[UTF8String]]()
    (0 until 100).foreach {
      _ => dataList2 = pattern2 :: dataList2
    }
    var data2 = dataList2.toArray

    val childExpression = MyBoundReference(0, StringType, nullable = false)
    val accuracy = 10000
    val frequency1 = (List(abc, abc), 400.0)
    val frequency2 = (List(abc, bcd), 300.0)
    val frequency3 = (List(bcd, abc), 200.0)
    val expectedNGrams = List(frequency1, frequency2, frequency3)
    val agg = new NGrams(childExpression, Literal(2), Literal(3))

    val group1Buffer = agg.createAggregationBuffer()
    data1.foreach((list: List[UTF8String]) => {
      val input = InternalRow(list: _*)
      agg.update(group1Buffer, input)
    })


    val group2Buffer = agg.createAggregationBuffer()
    data2.foreach((list: List[UTF8String]) => {
      val input = InternalRow(list: _*)
      agg.update(group2Buffer, input)
    })

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    val res = agg.eval(mergeBuffer)
    assert(res.equals(expectedNGrams))
  }

}
