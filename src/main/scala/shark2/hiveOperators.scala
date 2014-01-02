package catalyst
package shark2

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils
import org.apache.hadoop.hive.ql.plan.FileSinkDesc
import org.apache.hadoop.hive.serde2.objectinspector.{PrimitiveObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.`lazy`.LazyStruct
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.mapred.JobConf

import shark.execution.HadoopTableReader
import shark.SharkContext

import expressions.Attribute
import util._

/* Implicits */
import collection.JavaConversions._

case class HiveTableScan(attributes: Seq[Attribute], relation: MetastoreRelation) extends LeafNode {
  @transient
  val hadoopReader = new HadoopTableReader(relation.tableDesc, SharkContext.hiveconf)

  /**
   * The hive object inspector for this table, which can be used to extract values from the
   * serialized row representation.
   */
  @transient
  lazy val objectInspector =
    relation.tableDesc.getDeserializer.getObjectInspector.asInstanceOf[StructObjectInspector]

  /**
   * Functions that extract the requested attributes from the hive output.
   */
  @transient
  protected lazy val attributeFunctions = attributes.map { a =>
    if(relation.partitionKeys.contains(a)) {
      val ordinal = relation.partitionKeys.indexOf(a)
      (struct: LazyStruct, partitionKeys: Array[String]) => partitionKeys(ordinal)
    } else {
      val ref =
        objectInspector.getAllStructFieldRefs
        .find(_.getFieldName == a.name)
        .getOrElse(sys.error(s"Can't find attribute $a"))

      (struct: LazyStruct, _: Array[String]) => {
        val data = objectInspector.getStructFieldData(struct, ref)
        val inspector = ref.getFieldObjectInspector.asInstanceOf[PrimitiveObjectInspector]
        inspector.getPrimitiveJavaObject(data)
      }
    }
  }

  @transient
  def inputRdd =
    if(!relation.hiveQlTable.isPartitioned)
      hadoopReader.makeRDDForTable(relation.hiveQlTable)
    else
      hadoopReader.makeRDDForPartitionedTable(relation.hiveQlPartitions)

  def execute() = {
    inputRdd.map {
      case Array(struct: LazyStruct, partitionKeys: Array[String]) =>
        buildRow(attributeFunctions.map(_(struct, partitionKeys)))
      case struct: LazyStruct =>
        buildRow(attributeFunctions.map(_(struct, Array.empty)))
    }
  }

  def output = attributes
}

case class InsertIntoHiveTable(table: MetastoreRelation, child: SharkPlan)
                              (@transient sc: SharkContext) extends UnaryNode {
  /**
   * This file sink / record writer code is only the first step towards implementing this operator correctly and is not
   * actually used yet.
   */
  val desc = new FileSinkDesc("./", table.tableDesc, false)
  val outputClass = {
    val serializer = table.tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(null, table.tableDesc.getProperties)
    serializer.getSerializedClass
  }

  lazy val conf = new JobConf();
  lazy val writer = HiveFileFormatUtils.getHiveRecordWriter(
    conf,
    table.tableDesc,
    outputClass,
    desc,
    new Path((new org.apache.hadoop.fs.RawLocalFileSystem).getWorkingDirectory(), "test.out"))

  override def otherCopyArgs = sc :: Nil

  def output = child.output
  def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)

    // TODO: write directly to hive
    val tempDir = java.io.File.createTempFile("data", "tsv")
    tempDir.delete()
    tempDir.mkdir()
    childRdd.map(_.map(a => stringOrNull(a.asInstanceOf[AnyRef])).mkString("\001")).saveAsTextFile(tempDir.getCanonicalPath)
    sc.runSql(s"LOAD DATA LOCAL INPATH '${tempDir.getCanonicalPath}/*' INTO TABLE ${table.tableName}")

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    sc.makeRDD(Nil, 1)
  }
}