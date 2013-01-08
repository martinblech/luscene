package musicdb

import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexableField

class Index(indexWriter: IndexWriter) {

  def add(obj: Map[String, Seq[Any]]) {
    require(!obj.isEmpty)
    indexWriter addDocument mkDoc(obj)
  }

  def mkDoc(obj: Map[String, Seq[Any]]) = {
    val doc = new Document
    for {
      (k, vs) <- obj
      v <- vs
    } doc add mkField(k, v)
    doc
  }

  def mkField(k: String, v: Any): IndexableField =
    new TextField(k, v.toString, Field.Store.YES) // TODO make this configurable

}
