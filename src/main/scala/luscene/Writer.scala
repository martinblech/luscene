package luscene

import org.apache.lucene.index.{IndexWriter, Term}
import org.apache.lucene.search.Query

trait Writer { _: DocumentBuilder =>

  val indexWriter: IndexWriter
  val fieldStore: String => Boolean

  def checkObj(obj: IndexObject) =
    require(!obj.isEmpty)

  def add(obj: IndexObject) {
    checkObj(obj)
    indexWriter addDocument mkDoc(obj)
  }

  def update(fieldName: String, fieldValue: String, obj: IndexObject) {
    checkObj(obj)
    val doc = mkDoc(obj)
    val term = new Term(fieldName, fieldValue)
    indexWriter.updateDocument(term, doc)
  }

  def update(query: Query, obj: IndexObject) {
    checkObj(obj)
    delete(query)
    add(obj)
  }

  def delete(fieldName: String, fieldValue: String) {
    val term = new Term(fieldName, fieldValue)
    indexWriter deleteDocuments term
  }

  def delete(query: Query) {
    indexWriter deleteDocuments query
  }

  def close { indexWriter.close }

}
