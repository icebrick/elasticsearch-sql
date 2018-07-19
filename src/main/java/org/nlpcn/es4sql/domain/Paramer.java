package org.nlpcn.es4sql.domain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.exception.SqlParseException;



public class Paramer {
	public String analysis;
	public Float boost;
	public String value;
	public Integer slop;
	public String minimum_should_match;

    public Map<String, Float> fieldsBoosts = new HashMap<>();
    public String type;
    public Float tieBreaker;
    public Operator operator;

	public static Paramer parseParamer(SQLMethodInvokeExpr method) throws SqlParseException {
		Paramer instance = new Paramer();
		List<SQLExpr> parameters = method.getParameters();
		instance.value = ((SQLCharExpr) parameters.get(0)).getText();
		SQLExpr sqlExpr = null;
		for (int i = 1; i < parameters.size(); i++) {
			sqlExpr = parameters.get(i);
			if (sqlExpr instanceof SQLCharExpr) {
				instance.analysis = ((SQLCharExpr) sqlExpr).getText();
			} else if (sqlExpr instanceof SQLNumericLiteralExpr) {
				instance.boost = ((SQLNumericLiteralExpr) sqlExpr).getNumber().floatValue();
			}
			else if (sqlExpr instanceof SQLBinaryOpExpr) {
				SQLBinaryOpExpr sqlExprBO = (SQLBinaryOpExpr) sqlExpr;
			    switch (Util.expr2Object(sqlExprBO.getLeft()).toString()) {
					case "query":
						instance.value = Util.expr2Object(sqlExprBO.getRight()).toString();
						break;
					case "analyzer":
						instance.analysis = Util.expr2Object(sqlExprBO.getRight()).toString();
						break;
					case "boost":
						instance.boost = Float.parseFloat(Util.expr2Object(sqlExprBO.getRight()).toString());
						break;
					case "slop":
						instance.slop = Integer.parseInt(Util.expr2Object(sqlExprBO.getRight()).toString());
						break;
					case "minimum_should_match":
						instance.minimum_should_match = Util.expr2Object(sqlExprBO.getRight()).toString();
						break;

					case "fields":
						int index;
						for (String f : Strings.splitStringByCommaToArray(Util.expr2Object(sqlExprBO.getRight()).toString())) {
							index = f.lastIndexOf('^');
							if (-1 < index) {
								instance.fieldsBoosts.put(f.substring(0, index), Float.parseFloat(f.substring(index + 1)));
							} else {
								instance.fieldsBoosts.put(f, 1.0F);
							}
						}
						break;
					case "type":
						instance.type = Util.expr2Object(sqlExprBO.getRight()).toString();
						break;
					case "tie_breaker":
						instance.tieBreaker = Float.parseFloat(Util.expr2Object(sqlExprBO.getRight()).toString());
						break;
					case "operator":
						instance.operator = Operator.fromString(Util.expr2Object(sqlExprBO.getRight()).toString());
						break;
					default:
						break;
				}

			}
		}

		return instance;
	}

	public static ToXContent fullParamer(MatchPhraseQueryBuilder query, Paramer paramer) {
		if (paramer.analysis != null) {
			query.analyzer(paramer.analysis);
		}

		if (paramer.boost != null) {
			query.boost(paramer.boost);
		}
		return query;
	}

	public static ToXContent fullParamer(MatchQueryBuilder query, Paramer paramer) {
		if (paramer.analysis != null) {
			query.analyzer(paramer.analysis);
		}

		if (paramer.boost != null) {
			query.boost(paramer.boost);
		}
		if (paramer.minimum_should_match != null) {
			query.minimumShouldMatch(paramer.minimum_should_match);
		}
		return query;
	}

	public static ToXContent fullParamer(WildcardQueryBuilder query, Paramer paramer) {
		if (paramer.boost != null) {
			query.boost(paramer.boost);
		}
		return query;
	}

    public static ToXContent fullParamer(QueryStringQueryBuilder query, Paramer paramer) {
        if (paramer.analysis != null) {
            query.analyzer(paramer.analysis);
        }

        if (paramer.boost != null) {
            query.boost(paramer.boost);
        }
        return query;
    }
}
