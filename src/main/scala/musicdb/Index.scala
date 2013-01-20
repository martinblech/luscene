package musicdb

import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, IndexableField, Term}

class Index(indexWriter: IndexWriter,
            fieldStore: String => Boolean = _ => false) {

  def checkObj(obj: Map[String, Seq[Any]]) =
    require(!obj.isEmpty)

  def add(obj: Map[String, Seq[Any]]) {
    checkObj(obj)
    indexWriter addDocument mkDoc(obj)
  }

  def mkDoc(obj: Map[String, Seq[Any]]) = {
    val doc = new Document
    for {
      (name, values) <- obj
      value <- values
    } doc add mkField(name, value)
    doc
  }

  def mkField(name: String, value: Any): IndexableField = {
    lazy val store = if (fieldStore(name)) Field.Store.YES else Field.Store.NO
    value match {
      case s: String => new TextField(name, s, store)
      case i: Int => new IntField(name, i, store)
      case l: Long => new LongField(name, l, store)
      case f: Float => new FloatField(name, f, store)
      case d: Double => new DoubleField(name, d, store)
      case _ => throw new IllegalArgumentException(
        "don't know how to index '%s' (%s)".format(value, value.getClass)
      )
    }
  }

  def update(fieldName: String, fieldValue: Any, obj: Map[String, Seq[Any]]) {
    checkObj(obj)
    val term = mkTerm(fieldName, fieldValue)
    val doc = mkDoc(obj)
    indexWriter.updateDocument(term, doc)
  }

  def mkTerm(fieldName: String, fieldValue: Any) = fieldValue match {
    case s: String => new Term(fieldName, s)
    // TODO figure out what to do with numbers
    case _ => throw new IllegalArgumentException(
      "don't know how to make a term for '%s' (%s)".format(
        fieldValue, fieldValue.getClass)
    )
  }

  def delete(fieldName: String, fieldValue: Any) {
    val term = mkTerm(fieldName, fieldValue)
    indexWriter deleteDocuments term
  }

}
