package luscene

import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.specs2.mock._

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, Term}
import org.apache.lucene.search._
import org.apache.lucene.store.{Directory, RAMDirectory}
import org.apache.lucene.util.Version

import java.io.Reader

class LusceneSpec extends Specification with Mockito {

  "a document builder" should {
    isolated

    val index = new DocumentBuilder {
      override val fieldStore = mock[String => Boolean] defaultReturn true
      override val fieldIndex = mock[String => Boolean] defaultReturn true
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

    "make a field for a String" in {
      val value = "abc"
      val field = index.mkField("", value)
      field.stringValue must be_==(value)
      field.numericValue must beNull
    }

    "make a field for an Int" in {
      val value = Int.MaxValue
      val field = index.mkField("", value)
      field.numericValue must be_==(value)
      field.stringValue must be_==(value.toString)
    }

    "make a field for a Long" in {
      val value = Long.MaxValue
      val field = index.mkField("", value)
      field.numericValue must be_==(value)
      field.stringValue must be_==(value.toString)
    }

    "make a field for a Float" in {
      val value = Float.MaxValue
      val field = index.mkField("", value)
      field.numericValue must be_==(value)
      field.stringValue must be_==(value.toString)
    }

    "make a field for a Double" in {
      val value = Double.MaxValue
      val field = index.mkField("", value)
      field.numericValue must be_==(value)
      field.stringValue must be_==(value.toString)
    }

    "use the provided fieldIndex configuration" in {
      index.fieldIndex.apply("a") returns true
      index.fieldIndex.apply("b") returns false
      index.fieldIndex.apply("c") throws new NoSuchElementException
      index.mkField("a", "").fieldType.indexed must be_==(true)
      index.mkField("b", "").fieldType.indexed must be_==(false)
      index.mkField("c", "") must throwA[NoSuchElementException]
      there was one(index.fieldIndex).apply("a")
      there was one(index.fieldIndex).apply("b")
      there was one(index.fieldIndex).apply("c")
    }

    "use the provided fieldStore configuration" in {
      index.fieldStore.apply("a") returns true
      index.fieldStore.apply("b") returns false
      index.fieldStore.apply("c") throws new NoSuchElementException
      index.mkField("a", "").fieldType.stored must be_==(true)
      index.mkField("b", "").fieldType.stored must be_==(false)
      index.mkField("c", "") must throwA[NoSuchElementException]
      there was one(index.fieldStore).apply("a")
      there was one(index.fieldStore).apply("b")
      there was one(index.fieldStore).apply("c")
    }
  }

  "an index writer" should {
    isolated

    val index = new Writer with DocumentBuilder {
      override val indexWriter = mock[IndexWriter]
      override val fieldStore = mock[String => Boolean] defaultReturn true
      override val fieldIndex = mock[String => Boolean] defaultReturn true
    }

    "fail indexing an empty document" in {
      index.add(Map()) must throwA[IllegalArgumentException]
    }

    "index a simple document" in {
      val obj = Map("id" -> Seq("doc_a"))
      index.add(obj)
      val doc = capture[Document]
      there was one(index.indexWriter).addDocument(doc)
      doc.value.getFields.map(_.name).toSet must be_==(obj.keys)
    }

    "fail to update with an empty document" in {
      index.update("", "", Map()) must throwA[IllegalArgumentException]
    }

    "update a simple document with a string id" in {
      val obj = Map("id" -> Seq("bla"), "val" -> Seq("x"))
      val fieldName = "id"
      val fieldValue = "bla"
      index.update(fieldName, fieldValue, obj)
      val term = capture[Term]
      val doc = capture[Document]
      there was one(index.indexWriter).updateDocument(term, doc)
      term.value.field must be_==(fieldName)
      term.value.text must be_==(fieldValue)
      doc.value.getFields.map(_.name).toSet must be_==(obj.keys)
    }

    "update based on query" in {
      val obj = Map("id" -> Seq("bla"), "val" -> Seq("x"))
      val query = mock[Query]
      index.update(query, obj)
      val doc = capture[Document]
      there was one(index.indexWriter).deleteDocuments(query)
      there was one(index.indexWriter).addDocument(doc)
      doc.value.getFields.map(_.name).toSet must be_==(obj.keys)
    }

    "delete a document with a string id" in {
      val fieldName = "id"
      val fieldValue = "bla"
      index.delete(fieldName, fieldValue)
      val term = capture[Term]
      there was one(index.indexWriter).deleteDocuments(term)
      term.value.field must be_==(fieldName)
      term.value.text must be_==(fieldValue)
    }

    "delete a document by query" in {
      val query = mock[Query]
      index.delete(query)
      there was one(index.indexWriter).deleteDocuments(query)
    }

    "close the wrapped writer when asked to" in {
      index.close
      there was one(index.indexWriter).close
    }

  }

  "a query builder" should {
    isolated

    val index = new QueryBuilder {
      override val searchAnalyzer = new WhitespaceAnalyzer(Version.LUCENE_40)
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
  }

  "an object builder" should {
    isolated

    val index = new ObjectBuilder with DocumentBuilder {
      override val fieldStore = mock[String => Boolean] defaultReturn true
      override val fieldIndex = mock[String => Boolean] defaultReturn true
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

  "a searcher" should {
    isolated

    val searcher = mock[IndexSearcher]
    class TestSearcher extends Searcher with ObjectBuilder with DocumentBuilder {
      val fieldStore = mock[String => Boolean] defaultReturn true
      val fieldIndex = mock[String => Boolean] defaultReturn true
      def acquireSearcher = searcher
      def releaseSearcher(searcher: IndexSearcher) {}
    }
    val index = spy(new TestSearcher)

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
  }

  "a read/write index" should {

    "instantiate correctly with constructor" in {
      val indexWriter = mock[IndexWriter]
      val fieldStore = mock[String => Boolean]
      val fieldIndex = mock[String => Boolean]
      val searchAnalyzer = mock[Analyzer]
      val rwindex = new RWIndex(indexWriter, fieldStore, fieldIndex,
        searchAnalyzer, None)
      rwindex must beAnInstanceOf[RWIndex]
    }

    "instantiate correctly with config" in {
      todo
    }

  }

  "a read-only index" should {

    "instantiate correctly with constructor" in {
      val directory = mock[Directory]
      val searchAnalyzer = mock[Analyzer]
      val roindex = new ROIndex(directory, searchAnalyzer, None)
      roindex must beAnInstanceOf[ROIndex]
    }

    "instantiate correctly with config" in {
      todo
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
