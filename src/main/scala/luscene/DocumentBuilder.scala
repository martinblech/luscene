package luscene

import org.apache.lucene.document._
import org.apache.lucene.index._

trait DocumentBuilder {

  val fieldStore: String => Boolean
  val fieldIndex: String => Boolean

  def mkDoc(obj: IndexObject) = {
    val doc = new Document
    for {
      (name, values) <- obj
      value <- values
    } doc add mkField(name, value)
    doc
  }

  def mkField(name: String, value: Any): IndexableField = {
    def fieldType(ref: FieldType) = {
      val ft = new FieldType(ref)
      ft.setStored(fieldStore(name))
      ft.setIndexed(fieldIndex(name))
      ft
    }
    value match {
      case s: String =>
        new Field(name, s, fieldType(TextField.TYPE_STORED))
      case i: Int =>
        new IntField(name, i, fieldType(IntField.TYPE_STORED))
      case l: Long =>
        new LongField(name, l, fieldType(LongField.TYPE_STORED))
      case f: Float =>
        new FloatField(name, f, fieldType(FloatField.TYPE_STORED))
      case d: Double =>
        new DoubleField(name, d, fieldType(DoubleField.TYPE_STORED))
      case _ => throw new IllegalArgumentException(
        "don't know how to index '%s' (%s)".format(value, value.getClass))
    }
  }
}
