package luscene

import scala.collection.JavaConversions._

import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexableField

trait ObjectBuilder {

  def mkObj(doc: Document): IndexObject =
    for {
      (fieldName, fields) <- doc.groupBy(_.name)
    } yield (fieldName, fields.map(fieldValue _).toSeq)

  def fieldValue(field: IndexableField) = 
    Option(field.numericValue).getOrElse(field.stringValue)
}
