package musicdb

import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.specs2.specification.BeforeExample
import org.specs2.mock._

import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexableField

class IndexSpec extends Specification with Mockito with BeforeExample {
  def before {
    indexWriter = mock[IndexWriter]
    index = new Index(indexWriter)
  }
  var indexWriter: IndexWriter = null
  var index: Index = null

  "an index" should {
    "fail indexing an empty document" in {
      index.add(Map()) must throwA[IllegalArgumentException]
    }

    "index a simple document" in {
      val obj = Map("id" -> Seq("doc_a"))
      index.add(obj)
      val doc = capture[Document]
      there was one(indexWriter).addDocument(doc)
      doc.value.getFields.map(_.name) must contain("id") and have size(1)
    }

    "make a doc whose fields match the object's" in {
      val obj = Map(
        "a" -> Seq("1"),
        "b" -> Seq("2"),
        "c" -> Seq("3", "4", "5")
      )
      val doc = index.mkDoc(obj)
      // all doc's fields are in obj
      for {
        field <- doc
      } obj must haveKey(field.name)
      // all obj's properties are in doc
      for {
        (k, vs) <- obj
      } doc.toSeq.filter(_.name == k).length must be_==(vs.length)
    }

  }
}
