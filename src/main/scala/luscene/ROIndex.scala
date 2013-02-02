package luscene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.{IndexSearcher, SearcherFactory, SearcherManager}
import org.apache.lucene.store.Directory

class ROIndex(val directory: Directory,
              val searchAnalyzer: Analyzer,
              val searcherFactory: Option[SearcherFactory])
    extends Searcher with ObjectBuilder with QueryBuilder {

  lazy val searcherManager = new SearcherManager(
    directory, searcherFactory.getOrElse(null))

  def acquireSearcher = searcherManager.acquire

  def releaseSearcher(searcher: IndexSearcher) {
    searcherManager release searcher
  }
}

object ROIndex {
  def withConfig(config: IndexConfig) =
    new ROIndex(config.directory, config.searchAnalyzer, config.searcherFactory)
}
