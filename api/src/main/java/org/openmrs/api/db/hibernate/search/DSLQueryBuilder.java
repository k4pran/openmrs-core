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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.hibernate.NonUniqueResultException;
import org.hibernate.Session;
import org.hibernate.search.engine.search.predicate.SearchPredicate;
import org.hibernate.search.engine.search.predicate.dsl.BooleanPredicateClausesStep;
import org.hibernate.search.engine.search.predicate.dsl.SearchPredicateFactory;
import org.hibernate.search.engine.search.projection.SearchProjection;
import org.hibernate.search.engine.search.projection.dsl.SearchProjectionFactory;
import org.hibernate.search.mapper.orm.Search;
import org.hibernate.search.mapper.orm.session.SearchSession;
import org.openmrs.PatientIdentifier;
import org.openmrs.PersonAttribute;
import org.openmrs.PersonName;
import org.openmrs.collection.ListPart;

/**
 * Performs Lucene queries.
 * 
 * @since 1.11
 */
public abstract class DSLQueryBuilder<T> extends SearchQuery<T> {

	private final Map<String, Set<String>> includeTerms = new HashMap<>();
	private final Map<String, Set<String>> excludeTerms = new HashMap<>();
	
	private boolean noUniqueTerms = false;

	private Set<Object> skipSameValues;
	
	private String query;
	
	private MatchMode matchMode;
	
	private Collection<String> fields;
	
	private String[] projectedFields;

	boolean useOrQueryParser = false;

	private int resultOffset = 0;
	private int resultLimit = 100; // todo default ok?

	/**
	 * Normal uses a textual match algorithm for the search
	 * Soundex indicates to use a Phonetic search strategy
	 */
	public enum MatchType
	{
		NORMAL, SOUNDEX
	}

	public enum MatchMode
	{
		MATCH_ALL
	}
	
	public static <T> DSLQueryBuilder<T> newQuery(final Class<T> type, final Session session, final String query, final Collection<String> baseFieldNames) {
		return newQuery(type, session, query, baseFieldNames, MatchType.NORMAL);
	}
	
	public static <T> DSLQueryBuilder<T> newQuery(final Class<T> type, final Session session, final String query, final Collection<String> baseFieldNames, MatchType matchType) {
		
		return new DSLQueryBuilder<T>(type, session) {
			@Override
			protected SearchPredicate prepareQuery() {
				SearchPredicateFactory searchPredicateFactory = getSearchSession().scope(type).predicate();
				if (query.isEmpty()) {
					return searchPredicateFactory.matchAll().toPredicate();
				}
				Collection<String> fields = resolveIndexedFields(baseFieldNames, matchType);
				return searchPredicateFactory.match().fields(fields.toArray(new String[0]))
					.matching(query).toPredicate();
			}
		};
	}

	/**
	 * The preferred way to create a Lucene query using the query parser.
	 * @param type filters on type
	 * @param session
	 * @param query
	 * 
	 * @return the Lucene query
	 */
	public static <T> DSLQueryBuilder<T> newQuery(final Class<T> type, final Session session, final String query) {
		return new DSLQueryBuilder<T>(type, session) {
			
			@Override
			protected SearchPredicate prepareQuery() {
				SearchPredicateFactory searchPredicateFactory = getSearchSession().scope(type).predicate();
				if (query.isEmpty()) {
					return searchPredicateFactory.matchAll().toPredicate();
				}
//				return newQueryParser(searchPredicateFactory).parse(query); TODO
				return null;
			}
			
		};
	}
	
	/**
	 * Escape any characters that can be interpreted by the query parser.
	 * 
	 * @param query
	 * @return the escaped query
	 */
	public static String escapeQuery(final String query) {
		return QueryParser.escape(query);
	}
	
	public DSLQueryBuilder(Class<T> type, Session session) {
		super(session, type);
	}

	public DSLQueryBuilder<T> useOrQueryParser() {
		useOrQueryParser = true;

		return this;
	}

	/**
	 * Include items with the given value in the specified field.
	 * <p>
	 * It is a filter applied before the query.
	 * 
	 * @param field
	 * @param value
	 * @return the query
	 */
	public DSLQueryBuilder<T> include(String field, Object value) {
		if (value != null) {
			include(field, new Object[] { value });
		}
		
		return this;
	}
	
	public DSLQueryBuilder<T> include(String field, Collection<?> values) {
		if (values != null) {
			include(field, values.toArray());
		}
		
		return this;
	}
	
	/**
	 * Include items with any of the given values in the specified field.
	 * <p>
	 * It is a filter applied before the query.
	 * 
	 * @param field
	 * @param values
	 * @return the query
	 */
	public DSLQueryBuilder<T> include(String field, Object[] values) {
		if (field != null && values != null && values.length > 0) {
			Set<String> valueSet = includeTerms.computeIfAbsent(field, k -> new HashSet<>());
			for (Object value : values) {
				if (value != null) {
					valueSet.add(value.toString());
				}
			}
		}
		return this;
	}
	
	/**
	 * Exclude any items with the given value in the specified field.
	 * <p>
	 * It is a filter applied before the query.
	 * 
	 * @param field
	 * @param value
	 * @return the query
	 */
	public DSLQueryBuilder<T> exclude(String field, Object value) {
		if (value != null) {
			exclude(field, new Object[] { value });
		}
		
		return this;
	}
	
	/**
	 * Exclude any items with the given values in the specified field.
	 * <p>
	 * It is a filter applied before the query.
	 * 
	 * @param field
	 * @param values
	 * @return the query
	 */
	public DSLQueryBuilder<T> exclude(String field, Object[] values) {
		if (field != null && values != null && values.length > 0) {
			Set<String> valueSet = excludeTerms.computeIfAbsent(field, k -> new HashSet<>());
			for (Object value : values) {
				if (value != null) {
					valueSet.add(value.toString());
				}
			}
		}
		return this;
	}

	
	/**
	 * It is called by the constructor to get an instance of a query.
	 * <p>
	 * To construct the query you can use {@link #buildQuery()},
	 * which are created for the proper type.
	 * 
	 * @return the query
	 */
	protected abstract SearchPredicate prepareQuery();
	
	/**
	 * It is called by the constructor after creating {@link SearchQuery}.
	 * <p>
	 * You can override it to adjust the full text query, e.g. add a filter.
	 * 
	 * @param searchQuery
	 */
	protected void adjustFullTextQuery(org.hibernate.search.engine.search.query.SearchQuery<?> searchQuery) {
	}
	
	/**
	 * You can use it in {@link #prepareQuery()}.
	 * 
	 * @return the query parser
	 */
	protected SearchPredicate newQueryParser(SearchPredicateFactory f, Collection<String> baseFieldNames, String searchTerm) {
		BooleanPredicateClausesStep<?> bool = f.bool();
		for (String fieldName : baseFieldNames) {
			bool.should(f.match()
				.field(fieldName)
				.matching(searchTerm)
			);
		}

		return bool.toPredicate();
	}


	protected Collection<String> resolveIndexedFields(Collection<String> baseFieldNames, MatchType matchType) {
		String suffix;
		if(matchType == MatchType.SOUNDEX) {
			suffix = "Soundex"; // TODO find constants and check this will be consistent
		}
		else if (getType().isAssignableFrom(PatientIdentifier.class) || getType().isAssignableFrom(PersonName.class) || getType().isAssignableFrom(PersonAttribute.class)) {
			suffix = "Exact"; // TODO find constants and check this will be consistent
		} else {
			suffix = "Anywhere"; // TODO find constants and check this will be consistent, also not sure what default should be
		}

		final String finalSuffix = suffix;

		return baseFieldNames.stream()
			.map(baseName -> baseName + finalSuffix)
			.collect(Collectors.toList());
	}

	private void setDefaultOperator(QueryParser queryParser) {
		if (useOrQueryParser) {
			queryParser.setDefaultOperator(QueryParser.Operator.OR);
		} else {
			queryParser.setDefaultOperator(QueryParser.Operator.AND);
		}
	}


	/**
	 * Gives you access to the full text session.
	 * 
	 * @return the full text session
	 */
	protected SearchSession getSearchSession() {
		return Search.session(getSession());
	}
	
	/**
	 * Skip elements, values of which repeat in the given field.
	 * <p>
	 * Only first elements will be included in the results.
	 * <p>
	 * <b>Note:</b> This method must be called as last when constructing a query. When called it
	 * will project the query and create a filter to eliminate duplicates.
	 * 
	 * @param field
	 * @return this
	 */
	public DSLQueryBuilder<T> skipSame(String field){
		return skipSame(field, null);
	}

	/**
	 * Skip elements, values of which repeat in the given field.
	 * <p>
	 * Only first elements will be included in the results.
	 * <p>
	 * <b>Note:</b> This method must be called as last when constructing a query. When called it
	 * will project the query and create a filter to eliminate duplicates.
	 *
	 * @param field
	 * @param otherBuilder results of which should be skipped too. It works only for queries, which called skipSame as well.
	 * @return this
	 */
	public DSLQueryBuilder<T> skipSame(String field, DSLQueryBuilder<?> otherBuilder) {
		String idPropertyName = getSession().getSessionFactory()
			.getMetamodel()
			.entity(getType())
			.getId(Object.class)
			.getName();


		org.hibernate.search.engine.search.query.SearchQuery<List<?>> query = getSearchSession().search(getType())
			.select(f -> f.composite(buildCompositeProjection(f, field)))
			.where(f -> {
				SearchPredicate basePredicate = prepareQuery();
				SearchPredicate filterPredicate = buildIncludeExcludePredicates(f, includeTerms, excludeTerms);
				return f.bool(b -> {
					b.must(basePredicate);
					b.must(filterPredicate);
				});
			})
			.toQuery();

		List<List<?>> results = query.fetchAllHits();

		skipSameValues = new HashSet<>();
		if (otherBuilder != null) {
			if (otherBuilder.skipSameValues == null) {
				throw new IllegalArgumentException("The skipSame method must be called on the given DSLQueryBuilder before calling this method.");
			}
			skipSameValues.addAll(otherBuilder.skipSameValues);
		}

		Set<String> allowedIds = new HashSet<>();
//		for (Object[] row : results) {
//			Object id = row[0];
//			Object fieldValue = row[1];
//			if (fieldValue != null && skipSameValues.add(fieldValue)) {
//				allowedIds.add(id.toString());
//			}
//		}

//		if (allowedIds.isEmpty()) {
//			noUniqueTerms = true;
//		} else {
//			// You can store this set for later filtering in buildQuery() via id().matchingAny(...)
//			this.filteredIds = allowedIds;
//		} TODO no idea how to do this

		return this;
	}

	@Override
	public T uniqueResult() {
		if (noUniqueTerms) {
			return null;
		}

		@SuppressWarnings("unchecked")
		List<T> results = buildQuery().fetchHits(0, 2); // Fetch up to 2 in case there are multiple
		if (results.isEmpty()) {
			return null;
		}
		if (results.size() > 1) {
			throw new NonUniqueResultException(results.size());
		}
		return results.get(0);
	}
	
	@Override
	public List<T> list() {
		if (noUniqueTerms) {
			return Collections.emptyList();
		}

		@SuppressWarnings("unchecked")
		List<T> list = buildQuery().fetchAllHits();
		
		return list;
	}
	
	@Override
	public ListPart<T> listPart(Long firstResult, Long maxResults) {
		if (noUniqueTerms) {
			return ListPart.newListPart(Collections.emptyList(), firstResult, maxResults, 0L, true);
		}

		org.hibernate.search.engine.search.query.SearchQuery<T> searchQuery = buildQuery();
		applyPartialResults(firstResult, maxResults);
		
		@SuppressWarnings("unchecked")
		List<T> list = searchQuery.fetchAllHits();

//		return ListPart.newListPart(list, firstResult, maxResults, list.size(), true); TODO HOW ?
		return null;
	}
	
	/**
	 * @see org.openmrs.api.db.hibernate.search.SearchQuery#resultSize()
	 */
	@Override
	public long resultSize() {
		if (noUniqueTerms) {
			return 0;
		}

		return buildQuery().fetchTotalHitCount();
	}
	
	public List<Object[]> listProjection(String... fields) {
		if (noUniqueTerms) {
			return Collections.emptyList();
		}

		org.hibernate.search.engine.search.query.SearchQuery<List<?>> fullTextQuery = buildQuery(fields);
		
		@SuppressWarnings("unchecked")
		List<List<?>> list = fullTextQuery.fetchAllHits();

//		return list; TODO HOW?
		return null;
	}
	
	public ListPart<Object[]> listPartProjection(Long firstResult, Long maxResults, String... fields) {
		if (noUniqueTerms) {
			return ListPart.newListPart(Collections.emptyList(), firstResult, maxResults, 0L, true);
		}

		org.hibernate.search.engine.search.query.SearchQuery<List<?>> fullTextQuery = buildQuery(fields);
		applyPartialResults(firstResult, maxResults);
		
		List<List<?>> list = fullTextQuery.fetchHits(resultOffset, resultLimit);

//		return ListPart.newListPart(list, firstResult, maxResults, (long) list.size(), true); TODO HOW?
		return null;
	}
	
	public ListPart<Object[]> listPartProjection(Integer firstResult, Integer maxResults, String... fields) {
		Long first = (firstResult != null) ? Long.valueOf(firstResult) : null;
		Long max = (maxResults != null) ? Long.valueOf(maxResults) : null;
		return listPartProjection(first, max, fields);
	}
	
	private org.hibernate.search.engine.search.query.SearchQuery<T> buildQuery() {
		SearchPredicateFactory searchPredicateFactory = getSearchSession().scope(getType()).predicate();
		SearchPredicate basePredicate;
		
		basePredicate = prepareQuery();

		SearchPredicate filterPredicate = buildIncludeExcludePredicates(searchPredicateFactory, includeTerms, excludeTerms);

		SearchPredicate finalPredicate = searchPredicateFactory.bool(b -> {
			b.must(basePredicate);
			b.must(filterPredicate);
		}).toPredicate();
		
		// MatchAllDocsQuery
		org.hibernate.search.engine.search.query.SearchQuery<T> searchQuery = getSearchSession().search(getType())
			.where(finalPredicate)
			.toQuery();
		

		adjustFullTextQuery(searchQuery);

		return searchQuery;
	}

	private org.hibernate.search.engine.search.query.SearchQuery<List<?>> buildQuery(String... projectedFields) {
		SearchPredicateFactory searchPredicateFactory = getSearchSession().scope(getType()).predicate();
		SearchPredicate basePredicate;
		
		basePredicate = prepareQuery();

		SearchPredicate filterPredicate = buildIncludeExcludePredicates(searchPredicateFactory, includeTerms, excludeTerms);

		SearchPredicate finalPredicate = searchPredicateFactory.bool(b -> {
			b.must(basePredicate);
			b.must(filterPredicate);
		}).toPredicate();

		// MatchAllDocsQuery
		org.hibernate.search.engine.search.query.SearchQuery<List<?>> searchQuery = getSearchSession().search(getType())
			.select(f -> f.composite(buildCompositeProjection(f, projectedFields)))
			.where(finalPredicate)
			.toQuery();

		adjustFullTextQuery(searchQuery);

		return searchQuery;
	}
	
	private void applyPartialResults(Long firstResult, Long maxResults) {
		resultOffset = firstResult != null ? firstResult.intValue() : 0;
		resultLimit = maxResults != null ? maxResults.intValue() : 100;
	}

	public static <T> SearchPredicate buildIncludeExcludePredicates(
		SearchPredicateFactory pf,
		Map<String, Set<String>> includeTerms,
		Map<String, Set<String>> excludeTerms
	) {
		return pf.bool(bool -> {

			// Build include (must) clauses
			for (Map.Entry<String, Set<String>> entry : includeTerms.entrySet()) {
				String field = entry.getKey();
				Set<String> values = entry.getValue();
				if (!values.isEmpty()) {
					bool.must(inner -> inner.bool(innerBool -> {
						for (String value : values) {
							innerBool.should(pf.match().field(field).matching(value));
						}
					}));
				}
			}

			// Build exclude (mustNot) clauses
			for (Map.Entry<String, Set<String>> entry : excludeTerms.entrySet()) {
				String field = entry.getKey();
				Set<String> values = entry.getValue();
				for (String value : values) {
					bool.mustNot(pf.match().field(field).matching(value));
				}
			}

		}).toPredicate();
	}

	private SearchProjection<List<?>>[] buildCompositeProjection(SearchProjectionFactory f, String... fields) {
		if (fields == null || fields.length == 0) {
			return new SearchProjection[0];
		}
		return Arrays.stream(fields)
			.map(fieldName -> f.field(fieldName, Object.class))
			.toArray(SearchProjection[]::new);
	}
}
