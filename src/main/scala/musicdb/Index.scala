package musicdb

import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, IndexableField}

class Index(indexWriter: IndexWriter,
            fieldStore: String => Boolean = _ => false) {

  def add(obj: Map[String, Seq[Any]]) {
    require(!obj.isEmpty)
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

}
