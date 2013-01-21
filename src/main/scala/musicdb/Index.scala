package musicdb

import java.io.StringReader

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, IndexableField, Term}
import org.apache.lucene.search._

class Index(indexWriter: IndexWriter,
            fieldStore: String => Boolean,
            queryAnalyzer: Analyzer) {

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
        "don't know how to index '%s' (%s)".format(value, value.getClass))
    }
  }

  def update(fieldName: String, fieldValue: Any, obj: Map[String, Seq[Any]]) {
    fieldValue match {
      case s: String => {
        checkObj(obj)
        val doc = mkDoc(obj)
        val term = new Term(fieldName, s)
        indexWriter.updateDocument(term, doc)
      }
      case _ => {
        delete(fieldName, fieldValue)
        add(obj)
      }
    }
  }

  def delete(fieldName: String, fieldValue: Any) {
    fieldValue match {
      case s: String => {
        val term = new Term(fieldName, s)
        indexWriter deleteDocuments term
      }
      case _ => {
        val query = mkQuery(fieldName, fieldValue, fuzzy = false)
        indexWriter deleteDocuments query
      }
    }
  }

  def mkQuery(obj: Map[String, Seq[Any]], fuzzy: Boolean): Query = {
    val queries = for {
      (fieldName, fieldValues) <- obj
      fieldValue <- fieldValues
    } yield mkQuery(fieldName, fieldValue, fuzzy)
    queries match {
      case Nil => throw new IllegalArgumentException(
        "cannot make a query from an empty object"
      )
      case q :: Nil => q
      case _ => {
        val bq = new BooleanQuery
        val occur =
          if (fuzzy) BooleanClause.Occur.SHOULD
          else BooleanClause.Occur.MUST
        queries foreach { q =>
          bq add new BooleanClause(q, occur)
        }
        bq
      }
    }
  }

  def mkQuery(fieldName: String, fieldValue: Any, fuzzy: Boolean): Query =
    fieldValue match {
      case i: Int => NumericRangeQuery.newIntRange(fieldName, i, i, true, true)
      case l: Long => NumericRangeQuery.newLongRange(fieldName, l, l, true, true)
      case f: Float =>
        NumericRangeQuery.newFloatRange(fieldName, f, f, true, true)
      case d: Double =>
        NumericRangeQuery.newDoubleRange(fieldName, d, d, true, true)
      case s: String => {
        extractTokens(fieldName, s) match {
          case Nil => throw new IllegalArgumentException(
            "could not extract any tokens from '%s'".format(fieldValue))
          case Seq(t) => new TermQuery(new Term(fieldName, s))
          case tokens => {
            val pq = new PhraseQuery
            tokens foreach { token =>
              pq add new Term(fieldName, token)
            }
            pq
          }
        }
      }
      case _ => throw new IllegalArgumentException(
        "cannot make a query from '%s' (%s)".format(
          fieldValue, fieldValue.getClass))
    }

  def extractTokens(fieldName: String, fieldValue: String): Seq[String] = {
    val ts = queryAnalyzer.tokenStream(fieldName, new StringReader(fieldValue))
    val charTermAttr = ts.addAttribute(classOf[CharTermAttribute])
    val tokens = new scala.collection.mutable.MutableList[String]
    try {
      ts.reset
      while (ts.incrementToken) {
        tokens += charTermAttr.toString
      }
    } finally {
      ts.close
    }
    tokens
  }

}
