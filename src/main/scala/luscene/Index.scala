package luscene

import scala.collection.JavaConversions._

import java.io.{StringReader, File}

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.document._
import org.apache.lucene.index._
import org.apache.lucene.search._
import org.apache.lucene.store.Directory
import org.apache.lucene.util.Version

import com.twitter.util.Eval

class Index(indexWriter: IndexWriter,
            fieldStore: String => Boolean,
            searchAnalyzer: Analyzer,
            searcherFactory: Option[SearcherFactory]) {

  lazy val searcherManager = new SearcherManager(
    indexWriter, true, searcherFactory.getOrElse(null))

  type IndexObject = Map[String, Seq[Any]]

  def checkObj(obj: IndexObject) =
    require(!obj.isEmpty)

  def add(obj: IndexObject) {
    checkObj(obj)
    indexWriter addDocument mkDoc(obj)
  }

  def mkDoc(obj: IndexObject) = {
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

  def mkObj(doc: Document): IndexObject =
    for {
      (fieldName, fields) <- doc.groupBy(_.name)
    } yield (fieldName, fields.map(fieldValue _).toSeq)

  def fieldValue(field: IndexableField) = field match {
    case _: StringField | _: TextField =>
      field.stringValue
    case _: IntField | _: LongField | _: FloatField | _: DoubleField =>
      field.numericValue
    case _ => throw new IllegalArgumentException(
      "don't know how to extract value from field %s".format(field)
    )
  }

  def update(fieldName: String, fieldValue: Any, obj: IndexObject) {
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

  def mkQuery(obj: IndexObject, fuzzy: Boolean): Query = {
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
        for (q <- queries) bq add new BooleanClause(q, occur)
        bq
      }
    }
  }

  def mkQuery(fieldName: String, fieldValue: Any, fuzzy: Boolean): Query = {
    def numFuzzyCheck {
      if (fuzzy) throw new IllegalArgumentException(
        "can't make fuzzy queries over numeric fields (%s -> %s)".format(
          fieldName, fieldValue)
      )
    }
    fieldValue match {
      case i: Int => {
        numFuzzyCheck
        NumericRangeQuery.newIntRange(fieldName, i, i, true, true)
      }
      case l: Long => {
        numFuzzyCheck
        NumericRangeQuery.newLongRange(fieldName, l, l, true, true)
      }
      case f: Float => {
        numFuzzyCheck
        NumericRangeQuery.newFloatRange(fieldName, f, f, true, true)
      }
      case d: Double => {
        numFuzzyCheck
        NumericRangeQuery.newDoubleRange(fieldName, d, d, true, true)
      }
      case s: String => {
        val terms = extractTokens(fieldName, s) map (t => new Term(fieldName, t))
        require(!terms.isEmpty, "no tokens in '%s'".format(fieldValue))
        val mkTermQuery: Term => Query =
          if (fuzzy) new FuzzyQuery(_) else new TermQuery(_)
        if (terms.size == 1)
          mkTermQuery(terms(0))
        else {
          val pq = new PhraseQuery
          for (t <- terms) pq add t
          if (!fuzzy)
            pq
          else {
            val bq = new BooleanQuery
            val occur = BooleanClause.Occur.SHOULD
            bq add new BooleanClause(pq, occur)
            for (t <- terms) bq add new BooleanClause(mkTermQuery(t), occur)
            bq
          }
        }
      }
      case _ => throw new IllegalArgumentException(
        "cannot make a query from '%s' (%s)".format(
          fieldValue, fieldValue.getClass))
    }
  }

  def extractTokens(fieldName: String, fieldValue: String): Seq[String] = {
    val ts = searchAnalyzer.tokenStream(fieldName, new StringReader(fieldValue))
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

  def search(query: Query, offset: Int, length: Int,
      fields: Option[Set[String]]): SearchResults = {
    val s = acquireSearcher
    try {
      val topN = offset + length
      val topDocs = s.search(query, topN)
      val results = for {
        scoreDoc <- topDocs.scoreDocs.view(offset, offset+length)
      } yield {
        val obj = fields
          .map(f => if (f.isEmpty) None else Some(s.document(scoreDoc.doc, f)))
          .getOrElse(Some(s.doc(scoreDoc.doc)))
          .map(mkObj _)
        new SearchResult(scoreDoc.doc, scoreDoc.score, obj)
      }
      new SearchResults(topDocs.totalHits, results)
    } finally releaseSearcher(s)
  }

  def acquireSearcher = searcherManager.acquire

  def releaseSearcher(searcher: IndexSearcher) {
    searcherManager release searcher
  }

}

object Index {

  def fromConfig(config: IndexConfig): Index = {
    val indexWriter = new IndexWriter(config.directory, config.indexWriterConfig)
    new Index(indexWriter, config.fieldStore, config.searchAnalyzer,
      config.searcherFactory)
  }

  def fromConfig(configFile: File): Index = {
    val config: IndexConfig = new Eval()(configFile)
    fromConfig(config)
  }

}

case class SearchResults(totalHits: Int, entries: Seq[SearchResult])

case class SearchResult(docId: Int, score: Float,
  obj: Option[Map[String, Seq[Any]]])

trait IndexConfig {
  private def mkPerFieldAnalyzer(get: FieldSettings => Analyzer) = {
    val defaultAnalyzer = get(defaultField)
    val perFieldAnalyzers = for {
      (name, settings) <- fields
    } yield (name, get(settings))
    new PerFieldAnalyzerWrapper(defaultAnalyzer, perFieldAnalyzers)
  }

  lazy val indexAnalyzer: Analyzer = mkPerFieldAnalyzer(_.indexAnalyzer)
  lazy val searchAnalyzer: Analyzer = mkPerFieldAnalyzer(_.searchAnalyzer)

  private def mkMap[A](get: FieldSettings => A) = {
    (for {
      (name, settings) <- fields
    } yield (name, get(settings))) withDefaultValue get(defaultField)
  }

  lazy val fieldIndex: String => Boolean = mkMap(_.index)
  lazy val fieldStore: String => Boolean = mkMap(_.store)
  lazy val searcherFactory: Option[SearcherFactory] = None
  lazy val indexWriterConfig: IndexWriterConfig =
    new IndexWriterConfig(Version.LUCENE_40, indexAnalyzer)

  class FieldSettings {
    val analyzer: Analyzer = new WhitespaceAnalyzer(Version.LUCENE_40)
    lazy val indexAnalyzer: Analyzer = analyzer
    lazy val searchAnalyzer: Analyzer = analyzer
    val index: Boolean = true
    val store: Boolean = true
  }

  val fields: Map[String, FieldSettings] = Map()
  val defaultField: FieldSettings = new FieldSettings
  val directory: Directory
}
