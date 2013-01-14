package musicdb

import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.specs2.mock._

import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriter

class IndexSpec extends Specification with Mockito {
  isolated

  val indexWriter = mock[IndexWriter]
  val index = new Index(indexWriter)

  "an index" should {
    "fail indexing an empty document" in {
      index.add(Map()) must throwA[IllegalArgumentException]
    }

    "index a simple document" in {
      val obj = Map("id" -> Seq("doc_a"))
      index.add(obj)
      val doc = capture[Document]
      there was one(indexWriter).addDocument(doc)
      doc.value.getFields.map(_.name).toSet must be_==(obj.keys)
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

    "not know how to make a field from a List" in {
      index.mkField("", Nil) must throwA[IllegalArgumentException]
    }

    "make a TextField for a String" in {
      index.mkField("", "") must beAnInstanceOf[TextField]
    }

    "make a IntField for a Int" in {
      index.mkField("", 1) must beAnInstanceOf[IntField]
    }

    "make a LongField for a Long" in {
      index.mkField("", 1l) must beAnInstanceOf[LongField]
    }

    "make a FloatField for a Float" in {
      index.mkField("", 1f) must beAnInstanceOf[FloatField]
    }

    "make a DoubleField for a Double" in {
      index.mkField("", 1d) must beAnInstanceOf[DoubleField]
    }

    "use the provided fieldStore configuration" in {
      val fieldStore = spy(Map("a" -> true, "b" -> false))
      val index = new Index(indexWriter,
                            fieldStore = fieldStore)
      index.mkField("a", "").fieldType.stored must be_==(true)
      index.mkField("b", "").fieldType.stored must be_==(false)
      index.mkField("c", "") must throwA[NoSuchElementException]
      there was one(fieldStore).apply("a")
      there was one(fieldStore).apply("b")
      there was one(fieldStore).apply("c")
    }

  }
}
