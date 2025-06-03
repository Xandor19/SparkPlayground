package utils

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, MetadataBuilder}
import org.apache.spark.sql.types.DataTypes

class InformixDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:informix-sqli")

  /**
    * Gets the corresponding type known by Catalyst for 
    * a domain-specific type
    *
    * @param sqlType
    * @param typeName
    * @param size
    * @param md
    * @return
    */
  override def getCatalystType(
    sqlType: Int, 
    typeName: String, 
    size: Int, 
    md: MetadataBuilder
  ): Option[DataType] = typeName.toLowerCase() match {
    case "serial" => Some(DataTypes.IntegerType)
    case "calendar" => Some(DataTypes.BinaryType)
    case _ => None
  }
}