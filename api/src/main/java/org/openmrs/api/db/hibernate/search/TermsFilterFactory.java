package org.openmrs.api.db.hibernate.search;

import java.util.Set;

import org.hibernate.search.engine.search.predicate.SearchPredicate;
import org.hibernate.search.engine.search.predicate.dsl.SearchPredicateFactory;

public final class TermsFilterFactory {

	private TermsFilterFactory() {
		// Utility class
	}

	public static SearchPredicate create(SearchPredicateFactory f,
										 Set<Set<FieldAndValue>> includeTerms,
										 Set<FieldAndValue> excludeTerms) {

		return f.bool(b -> {
			// Handle inclusions
			if (includeTerms == null || includeTerms.isEmpty()) {
				b.must(f.matchAll());
			} else {
				for (Set<FieldAndValue> termGroup : includeTerms) {
					if (termGroup.size() == 1) {
						FieldAndValue term = termGroup.iterator().next();
						b.must(f.match().field(term.getField()).matching(term.getValue()));
					} else {
						b.must(f.bool(sub -> {
							for (FieldAndValue term : termGroup) {
								sub.should(f.match().field(term.getField()).matching(term.getValue()));
							}
						}));
					}
				}
			}

			// Handle exclusions
			if (excludeTerms != null) {
				for (FieldAndValue term : excludeTerms) {
					b.mustNot(f.match().field(term.getField()).matching(term.getValue()));
				}
			}
		}).toPredicate();
	}
}
