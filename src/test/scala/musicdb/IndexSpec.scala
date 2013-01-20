package musicdb

import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.specs2.mock._

import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, Term}
import org.apache.lucene.search._

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

    "fail to update with an empty document" in {
      index.update("", "", Map()) must throwA[IllegalArgumentException]
    }

    "fail to update with bad id values" in {
      val obj = Map("a" -> Seq("b"))
      index.update("", Nil, obj) must throwA[IllegalArgumentException]
      (index.update("", new java.math.BigDecimal(1), obj)
        must throwA[IllegalArgumentException])
    }

    "update a simple document with a string id" in {
      val obj = Map("id" -> Seq("bla"), "val" -> Seq("x"))
      val fieldName = "id"
      val fieldValue = "bla"
      index.update(fieldName, fieldValue, obj)
      val term = capture[Term]
      val doc = capture[Document]
      there was one(indexWriter).updateDocument(term, doc)
      term.value.field must be_==(fieldName)
      term.value.text must be_==(fieldValue)
      doc.value.getFields.map(_.name).toSet must be_==(obj.keys)
    }

    "update a simple document with a numeric id" in {
      val obj = Map("id" -> Seq("bla"), "val" -> Seq("x"))
      val fieldName = "id"
      val fieldValue = 1
      index.update(fieldName, fieldValue, obj)
      val query = capture[NumericRangeQuery[Integer]]
      val doc = capture[Document]
      there was one(indexWriter).deleteDocuments(query)
      query.value.includesMin must be_==(true)
      query.value.includesMax must be_==(true)
      query.value.getMin must be_==(fieldValue)
      query.value.getMax must be_==(fieldValue)
      there was one(indexWriter).addDocument(doc)
      doc.value.getFields.map(_.name).toSet must be_==(obj.keys)
    }

    "fail to delete with bad id values" in {
      index.delete("", Nil) must throwA[IllegalArgumentException]
      (index.delete("", new java.math.BigDecimal(1))
        must throwA[IllegalArgumentException])
    }

    "delete a document with a string id" in {
      val fieldName = "id"
      val fieldValue = "bla"
      index.delete(fieldName, fieldValue)
      val term = capture[Term]
      there was one(indexWriter).deleteDocuments(term)
      term.value.field must be_==(fieldName)
      term.value.text must be_==(fieldValue)
    }

    "delete a document with a numeric id" in {
      val fieldName = "id"
      val fieldValue = 1f
      index.delete(fieldName, fieldValue)
      val query = capture[NumericRangeQuery[java.lang.Float]]
      there was one(indexWriter).deleteDocuments(query)
      query.value.includesMin must be_==(true)
      query.value.includesMax must be_==(true)
      query.value.getMin must be_==(fieldValue)
      query.value.getMax must be_==(fieldValue)
    }

    "fail making a query for an empty object" in {
      index.mkQuery(Map(), fuzzy = true) must throwA[IllegalArgumentException]
    }

    "make a non-fuzzy query from a single-field, single-word object" in {
      val fieldName = "a"
      val fieldValue = "hello"
      val obj = Map(fieldName -> Seq(fieldValue))
      index.mkQuery(obj, fuzzy = false) match {
        case tq: TermQuery => {
          tq.getTerm.field must be_==(fieldName)
          tq.getTerm.text must be_==(fieldValue)
        }
      }
    }

    "make a non-fuzzy query for a multi-field, single-word object" in {
      val obj = Map("a" -> Seq("hello"), "b" -> Seq("world"))
      index.mkQuery(obj, fuzzy = false) match {
        case bq: BooleanQuery => {
          for (clause <- bq) {
            clause.getOccur must be_==(BooleanClause.Occur.MUST)
            clause.getQuery must beAnInstanceOf[TermQuery]
          }
        }
      }
    }

    // TODO multi-word queries (requires search analyzer)
  }
}
