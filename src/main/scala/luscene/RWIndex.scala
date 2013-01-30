package luscene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{IndexSearcher, SearcherFactory, SearcherManager}

class RWIndex(val indexWriter: IndexWriter,
              val fieldStore: String => Boolean,
              val fieldIndex: String => Boolean,
              val searchAnalyzer: Analyzer,
              val searcherFactory: Option[SearcherFactory])
    extends DocumentBuilder
    with Writer
    with QueryBuilder
    with ObjectBuilder
    with Searcher {

  lazy val searcherManager = new SearcherManager(
    indexWriter, true, searcherFactory.getOrElse(null))

  def acquireSearcher = searcherManager.acquire

  def releaseSearcher(searcher: IndexSearcher) {
    searcherManager release searcher
  }

}

object RWIndex {
  def withConfig(config: IndexConfig) = {
    val indexWriter = new IndexWriter(config.directory, config.indexWriterConfig)
    new RWIndex(indexWriter, config.fieldStore, config.fieldIndex,
      config.searchAnalyzer, config.searcherFactory)
  }
}
