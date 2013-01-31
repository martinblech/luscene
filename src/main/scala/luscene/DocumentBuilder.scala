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
    def fieldType(cls: Class[_ <: IndexableField]) = {
      val stored = fieldStore(name)
      val indexed = fieldIndex(name)
      DocumentBuilder.fieldTypeMap((cls, stored, indexed))
    }
    value match {
      case s: String =>
        new Field(name, s, fieldType(classOf[TextField]))
      case i: Int =>
        new IntField(name, i, fieldType(classOf[IntField]))
      case l: Long =>
        new LongField(name, l, fieldType(classOf[LongField]))
      case f: Float =>
        new FloatField(name, f, fieldType(classOf[FloatField]))
      case d: Double =>
        new DoubleField(name, d, fieldType(classOf[DoubleField]))
      case _ => throw new IllegalArgumentException(
        "don't know how to index '%s' (%s)".format(value, value.getClass))
    }
  }
}

object DocumentBuilder {
  val fieldTypeMap: Map[(Class[_], Boolean, Boolean), FieldType] = Map(
    // TextField
    (classOf[TextField], true, true) -> TextField.TYPE_STORED,
    (classOf[TextField], true, false) -> {
      val ft = new FieldType(TextField.TYPE_STORED)
      ft.setIndexed(false)
      ft
    },
    (classOf[TextField], false, true) -> TextField.TYPE_NOT_STORED,
    // IntField
    (classOf[IntField], true, true) -> IntField.TYPE_STORED,
    (classOf[IntField], true, false) -> {
      val ft = new FieldType(IntField.TYPE_STORED)
      ft.setIndexed(false)
      ft
    },
    (classOf[IntField], false, true) -> IntField.TYPE_NOT_STORED,
    // LongField
    (classOf[LongField], true, true) -> LongField.TYPE_STORED,
    (classOf[LongField], true, false) -> {
      val ft = new FieldType(LongField.TYPE_STORED)
      ft.setIndexed(false)
      ft
    },
    (classOf[LongField], false, true) -> LongField.TYPE_NOT_STORED,
    // FloatField
    (classOf[FloatField], true, true) -> FloatField.TYPE_STORED,
    (classOf[FloatField], true, false) -> {
      val ft = new FieldType(FloatField.TYPE_STORED)
      ft.setIndexed(false)
      ft
    },
    (classOf[FloatField], false, true) -> FloatField.TYPE_NOT_STORED,
    // DoubleField
    (classOf[DoubleField], true, true) -> DoubleField.TYPE_STORED,
    (classOf[DoubleField], true, false) -> {
      val ft = new FieldType(DoubleField.TYPE_STORED)
      ft.setIndexed(false)
      ft
    },
    (classOf[DoubleField], false, true) -> DoubleField.TYPE_NOT_STORED
  )
}
