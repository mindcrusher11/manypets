package org.manypets.cam
package utils

import javassist.bytecode.stackmap.TypeTag
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.{col, explode, explode_outer}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.annotation.tailrec
import scala.reflect.ClassTag

object HelpersFunctions {
  def flattenDataframe(df: DataFrame): DataFrame = {
    //getting all the fields from schema
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    //length shows the number of fields inside dataframe
    val length = fields.length
    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldName1 = fieldName
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName1)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName1) as $fieldName1")
          //val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName1.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flattenDataframe(explodedDf)

        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("$", "_").replace("__", "_").replace(" ", "").replace("-", ""))))
          val explodedf = df.select(renamedcols: _*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }

  @tailrec
  def recurs(df: DataFrame): DataFrame = {
    if (df.schema.fields.find(_.dataType match {
      case ArrayType(StructType(_), _) | StructType(_) => true
      case _ => false
    }).isEmpty) df
    else {
      val columns = df.schema.fields.map(f => f.dataType match {
        case _: ArrayType => explode(col(f.name)).as(f.name)
        case s: StructType => col(s"${f.name}.*")
        case _ => col(f.name)
      })
      recurs(df.select(columns: _*))
    }
  }


  /*def getClaimSchema[T <: Product : ClassTag]: StructType ={
    val encoderSchema = Encoders.product[T].schema
    encoderSchema.printTreeString()

    import org.apache.spark.sql.catalyst.ScalaReflection
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    schema
  }*/
}

