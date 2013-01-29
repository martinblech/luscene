package luscene

import scala.collection.JavaConversions._

import java.io.File

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.search.SearcherFactory
import org.apache.lucene.store.Directory
import org.apache.lucene.util.Version

import com.twitter.util.Eval

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

object IndexConfig {
  def fromFile(file: File): IndexConfig = new Eval()(file)
}
