package luscene

import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.specs2.mock._

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, Term}
import org.apache.lucene.search._
import org.apache.lucene.store.RAMDirectory
import org.apache.lucene.util.Version

import java.io.Reader

class TestIndex(indexWriter: IndexWriter, fieldStore: String => Boolean,
                searchAnalyzer: Analyzer, searcher: IndexSearcher)
    extends Index(indexWriter, fieldStore, searchAnalyzer, None) {
  override def acquireSearcher = searcher
  override def releaseSearcher(s: IndexSearcher) {}
}

class IndexSpec extends Specification with Mockito {

  "an index" should {
    isolated

    val indexWriter = mock[IndexWriter]
    val fieldStore = (_: String) => false
    val searchAnalyzer = new WhitespaceAnalyzer(Version.LUCENE_40)
    val searcher = mock[IndexSearcher]
    val index = spy(
      new TestIndex(indexWriter, fieldStore, searchAnalyzer, searcher)
    )

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

    "use the provided fieldIndex configuration" in {
      todo // TODO
    }

    "use the provided fieldStore configuration" in {
      val fieldStore = spy(Map("a" -> true, "b" -> false))
      val index = new Index(indexWriter, fieldStore, null, None)
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

    "fail making a query for an object with an empty value" in {
      val obj = Map("a" -> Nil)
      index.mkQuery(obj, fuzzy = true) must throwA[IllegalArgumentException]
    }

    "fail making a query for an object with an empty string value" in {
      val obj = Map("a" -> Seq(""))
      index.mkQuery(obj, fuzzy = true) must throwA[IllegalArgumentException]
    }


    "make an exact query for a single-field, single-word object" in {
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

    "make an exact query for a multi-field, single-word object" in {
      val obj = Map("a" -> Seq("hello"), "b" -> Seq("world"))
      index.mkQuery(obj, fuzzy = false) match {
        case bq: BooleanQuery => {
          bq.size must be_==(obj.size)
          for (clause <- bq) {
            clause.getOccur must be_==(BooleanClause.Occur.MUST)
            clause.getQuery must beAnInstanceOf[TermQuery]
          }
        }
      }
    }

    "make an exact query for a single-field, multi-word object" in {
      val obj = Map("a" -> Seq("hello world"))
      index.mkQuery(obj, fuzzy = false) match {
        case pq: PhraseQuery => {
          val terms = pq.getTerms
          terms.length must be_==(2)
          terms(0).text must be_==("hello")
          terms(1).text must be_==("world")
        }
      }
    }

    "make an exact query for a multi-field, multi-word object" in {
      val obj = Map("a" -> Seq("hello world"), "b" -> Seq("hi there"))
      index.mkQuery(obj, fuzzy = false) match {
        case bq: BooleanQuery => {
          bq.size must be_==(obj.size)
          for (clause <- bq) {
            clause.getOccur must be_==(BooleanClause.Occur.MUST)
            clause.getQuery must beAnInstanceOf[PhraseQuery]
          }
        }
      }
    }

    "make a fuzzy query for a single-field, single-word object" in {
      val obj = Map("a" -> Seq("hello"))
      index.mkQuery(obj, fuzzy = true) match {
        case fq: FuzzyQuery => {
          val term = fq.getTerm
          term.field must be_==("a")
          term.text must be_==("hello")
        }
      }
    }

    "make a fuzzy query for a multi-field, single-word object" in {
      val obj = Map("a" -> Seq("hello"), "b" -> Seq("world"))
      index.mkQuery(obj, fuzzy = true) match {
        case bq: BooleanQuery => {
          bq.size must be_==(obj.size)
          for (clause <- bq) {
            clause.getOccur must be_==(BooleanClause.Occur.SHOULD)
            clause.getQuery must beAnInstanceOf[FuzzyQuery]
          }
        }
      }
    }

    "make a fuzzy query for a single-field, multi-word object" in {
      val obj = Map("a" -> Seq("hello world"))
      index.mkQuery(obj, fuzzy = true) match {
        case bq: BooleanQuery => {
          for (bc <- bq) bc.getOccur must be_==(BooleanClause.Occur.SHOULD)
          val (pqs, fqs) = bq.map(_.getQuery).partition(
            _.isInstanceOf[PhraseQuery])
          pqs.size must be_==(1)
          pqs.head match { case pq: PhraseQuery =>
            val terms = pq.getTerms
            terms.length must be_==(2)
            terms(0).text must be_==("hello")
            terms(1).text must be_==("world")
          }
          fqs.size must be_==(2)
          for (fq <- fqs) fq must beAnInstanceOf[FuzzyQuery]
        }
      }
    }

    "make a fuzzy query for a multi-field, multi-word object" in {
      val obj = Map("a" -> Seq("hello world"), "b" -> Seq("hi there"))
      index.mkQuery(obj, fuzzy = true) match {
        case bq: BooleanQuery => {
          bq.size must be_==(obj.size)
          for (clause <- bq) {
            clause.getOccur must be_==(BooleanClause.Occur.SHOULD)
            clause.getQuery match { case innerbq: BooleanQuery =>
              innerbq.size must be_==(3)
            }
          }
        }
      }
    }

    "create an exact query for a numeric value" in {
      val q = index.mkQuery(Map("a" -> Seq(1)), fuzzy = false)
      q must beAnInstanceOf[NumericRangeQuery[java.lang.Integer]]
    }

    "fail creating a fuzzy query for a numeric value" in {
      (index.mkQuery(Map("a" -> Seq(1)), fuzzy=true)
        must throwAn[IllegalArgumentException])
    }

    "release the searcher when the query goes ok" in {
      searcher.search(any[Query], anyInt) returns new TopDocs(0, Array(), 0f)
      index.search(null, 0, 0, None)
      there was one(index).releaseSearcher(searcher)
    }

    "release the searcher even when the query fails" in {
      val exc = new RuntimeException("query failed")
      searcher.search(any[Query], anyInt) throws exc
      index.search(null, 0, 0, None) must throwAn(exc)
      there was one(index).releaseSearcher(searcher)
    }

    "search the index and load result objects correctly" in {
      val query = mock[Query]
      val totalHits = 100
      val offset = 1
      val length = 2
      val topDocs = new TopDocs(
        totalHits,
        Array(
          new ScoreDoc(0, 10f),
          new ScoreDoc(1, 5f),
          new ScoreDoc(2, 1f)
        ),
        10f
      )
      searcher.search(query, offset + length) returns topDocs
      searcher.doc(1) returns index.mkDoc(Map("a" -> Seq("hello world")))
      searcher.doc(2) returns index.mkDoc(Map("b" -> Seq("what's up", 1)))
      val results = index.search(query, offset, length, None)
      there was one(searcher).search(query, offset + length)
      results.totalHits must be_==(totalHits)
      results.entries.length must be_==(2)
      results.entries(0) must be_==(
        SearchResult(1, 5f, Some(Map("a" -> Seq("hello world")))))
      results.entries(1) must be_==(
        SearchResult(2, 1f, Some(Map("b" -> Seq("what's up", 1)))))
    }

    "make an object from a doc with a string field" in {
      val obj = Map("a" -> Seq("b"))
      index.mkObj(index.mkDoc(obj)) must be_==(obj)
    }

    "make an object from a doc with a numeric field" in {
      val obj = Map("a" -> Seq(1))
      index.mkObj(index.mkDoc(obj)) must be_==(obj)
    }

    "roundtrip object<->doc with many fields" in {
      val obj = Map(
        "a" -> Seq("a", "b", "c"),
        "b" -> Seq(1, 2l, 3f, 4d),
        "c" -> Seq("a", 1, "b", 2l, "c", 3f, "d", 4d)
      )
      index.mkObj(index.mkDoc(obj)) must be_==(obj)
    }

  }

  "an index config object" should {

    "have correct default values" in {
      val cfg = new IndexConfig {
        override val directory = new RAMDirectory
      }
      todo // TODO
    }

    "load from a file correctly" in {
      todo // TODO
    }

  }
}
