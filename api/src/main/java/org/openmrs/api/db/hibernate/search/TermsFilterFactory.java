/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.api.db.hibernate.search;

import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.hibernate.search.backend.lucene.LuceneExtension;
import org.hibernate.search.engine.search.predicate.SearchPredicate;
import org.hibernate.search.engine.search.predicate.dsl.BooleanPredicateClausesStep;
import org.hibernate.search.engine.search.predicate.dsl.SearchPredicateFactory;

public class TermsFilterFactory {

	public static SearchPredicate getQuery(SearchPredicateFactory f, Query baseLuceneQuery, Query includeTermsQuery, Set<Set<Term>> includeTerms, Set<Term> excludeTerms) {
		BooleanPredicateClausesStep<?> rootBool = f.bool();

		rootBool.must(
			f.extension( LuceneExtension.get() )
				.fromLuceneQuery( baseLuceneQuery )
				.toPredicate()
		);

		rootBool.must(
			f.extension( LuceneExtension.get() )
				.fromLuceneQuery( includeTermsQuery )
				.toPredicate()
		);

		for (Set<Term> terms : includeTerms) {
			if (terms.size() == 1) {
				Term t = terms.iterator().next();
				rootBool.must(
					f.match().field( t.field() ).matching( t.text() )
				);
			} else {
				rootBool.must(
					f.bool(inner -> {
						for (Term t : terms) {
							inner.should(
								f.match().field(t.field()).matching(t.text())
							);
						}
					})
				);
			}
		}

		for (Term t : excludeTerms) {
			rootBool.mustNot(
				f.match().field(t.field()).matching(t.text())
			);
		}

		return rootBool.toPredicate();
	}
}
