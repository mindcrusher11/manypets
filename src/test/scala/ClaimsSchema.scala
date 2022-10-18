package org.manypets.cam

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType
import org.manypets.cam.models.PolicyClaim

object ClaimsSchema {

  /*  def getClaimsSchema(): Unit = {

    val encoderSchema = Encoders.product[PolicyClaim].schema
    encoderSchema.printTreeString()

    import org.apache.spark.sql.catalyst.ScalaReflection
    val schema =
      ScalaReflection.schemaFor[PolicyClaim].dataType.asInstanceOf[StructType]
  }*/

  def getClaimsSchema[T <: Product: TypeTag](): StructType = {

    //implicit val encoderSchema = Encoders.product[T].schema
    //encoderSchema.printTreeString()

    import org.apache.spark.sql.catalyst.ScalaReflection
    val schema = {
      ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    }
    schema
  }
}
