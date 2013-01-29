package luscene

import java.io.StringReader

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.index.Term
import org.apache.lucene.search._

trait QueryBuilder {

  val searchAnalyzer: Analyzer

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
}
