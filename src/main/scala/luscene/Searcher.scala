package luscene

import scala.collection.JavaConversions._

import org.apache.lucene.search.{IndexSearcher, Query}

trait Searcher { _: ObjectBuilder =>

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

  def acquireSearcher: IndexSearcher
  def releaseSearcher(searcher: IndexSearcher)

}

case class SearchResults(totalHits: Int, entries: Seq[SearchResult])

case class SearchResult(docId: Int, score: Float, obj: Option[IndexObject])
