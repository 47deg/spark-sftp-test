package org.fortysevendeg.sparksftp.common

import scala.language.existentials

object RegisterInKryo {

  lazy val classes = List(
    classOf[org.apache.spark.sql.execution.command.PartitionStatistics],
    "org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableBlockLocation".mkClass,
    Class.forName(
      "[Lorg.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableBlockLocation;"
    ),
    "org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableFileStatus".mkClass,
    "org.apache.spark.sql.types.Decimal$DecimalIsFractional$".mkClass,
    "org.apache.spark.sql.types.Decimal$DecimalAsIfIntegral$".mkClass,
    classOf[org.apache.spark.sql.types.DecimalType],
    "org.apache.spark.sql.execution.joins.LongToUnsafeRowMap".mkClass,
    "org.apache.spark.sql.execution.joins.LongHashedRelation".mkClass,
    classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
    classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
    "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage".mkClass,
    classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
    "org.apache.spark.sql.types.IntegerType$".mkClass,
    "org.apache.spark.sql.types.NullType$".mkClass,
    classOf[Array[org.apache.spark.sql.types.DataType]],
    classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
    classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
    classOf[org.apache.spark.sql.types.StructType],
    classOf[org.apache.spark.sql.types.StructField],
    classOf[Array[org.apache.spark.sql.types.StructField]],
    "org.apache.spark.sql.types.StringType$".mkClass,
    "scala.collection.immutable.Map$EmptyMap$".mkClass,
    "org.apache.spark.sql.types.LongType$".mkClass,
    classOf[org.apache.spark.sql.types.ArrayType],
    classOf[org.apache.spark.sql.types.Metadata],
    classOf[org.apache.spark.sql.catalyst.InternalRow],
    classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
    classOf[Array[org.apache.spark.sql.catalyst.expressions.UnsafeRow]],
    classOf[scala.collection.immutable.Range],
    "org.apache.spark.sql.execution.joins.UnsafeHashedRelation".mkClass,
    "java.util.HashMap".mkClass,
    "java.lang.Class".mkClass,
    "org.apache.spark.sql.execution.columnar.CachedBatch".mkClass,
    "[[B".mkClass
  )

  implicit class RichClassForName(cls: String) {
    def mkClass = Class.forName(cls)
  }
}
