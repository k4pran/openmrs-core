/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 * 
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.api.db;

import java.util.Collection;

import jakarta.persistence.EntityManager;
import org.hibernate.Session;
import org.hibernate.engine.spi.SessionDelegatorBaseImpl;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.search.engine.search.query.dsl.SearchQuerySelectStep;
import org.hibernate.search.mapper.orm.automaticindexing.session.AutomaticIndexingSynchronizationStrategy;
import org.hibernate.search.mapper.orm.common.EntityReference;
import org.hibernate.search.mapper.orm.schema.management.SearchSchemaManager;
import org.hibernate.search.mapper.orm.scope.SearchScope;
import org.hibernate.search.mapper.orm.search.loading.dsl.SearchLoadingOptionsStep;
import org.hibernate.search.mapper.orm.session.SearchSession;
import org.hibernate.search.mapper.orm.work.SearchIndexingPlan;
import org.hibernate.search.mapper.orm.work.SearchWorkspace;
import org.hibernate.search.mapper.pojo.work.IndexingPlanSynchronizationStrategy;
import org.hibernate.search.mapper.pojo.work.SearchIndexingPlanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

/**
 * Custom implementation of the {@link SearchSession} interface that acts a wrapper around a
 * target FullTextSession instance, it actually delegates all the method calls directly to the
 * target except for the {@link SearchSession#search(Collection)} (Query, Class[]) method where it
 * first notifies registered listeners of the creation event before returning the newly created
 * {@link SearchQuerySelectStep} object. The newly created query object and entity type are passed to the
 * listeners wrapped in a {@link FullTextQueryAndEntityClass} object. <br>
 * <br>
 * An example use case is that a listener can enable/disable filters on the newly created query
 * object.
 */
public class DelegatingFullTextSession extends SessionDelegatorBaseImpl implements SearchSession {
	
	private static final Logger log = LoggerFactory.getLogger(DelegatingFullTextSession.class);
	
	private SearchSession delegate;
	
	private ApplicationEventPublisher eventPublisher;
	
	public DelegatingFullTextSession(SearchSession delegate, ApplicationEventPublisher eventPublisher) {
		super((SessionImplementor) delegate);
		this.delegate = delegate;
		this.eventPublisher = eventPublisher;
	}

	@Override
	public <T> SearchQuerySelectStep<?, EntityReference, T, SearchLoadingOptionsStep, ?, ?> search(Class<T> clazz) {
		log.debug("Creating new SearchQuerySelectStep instance");
		
		SearchQuerySelectStep<?, EntityReference, T, SearchLoadingOptionsStep, ?, ?> query = delegate.search(clazz);

		log.debug("Notifying FullTextQueryCreated listeners...");

		//Notify listeners, note that we intentionally don't catch any exception from a listener
		//so that failure should just halt the entire creation operation, this is possible because 
		//the default ApplicationEventMulticaster in spring fires events serially in the same thread
		//but has the downside of where a rogue listener can block the entire application.
//		FullTextQueryAndEntityClass queryAndClass = new FullTextQueryAndEntityClass(query, clazz); // todo 
//		eventPublisher.publishEvent(new FullTextQueryCreatedEvent(queryAndClass));
		
		return query;
	}

	@Override
	public <T> SearchQuerySelectStep<?, EntityReference, T, SearchLoadingOptionsStep, ?, ?> search(Collection<? extends Class<? extends T>> entities) {
		if (entities.size() > 1) {
			throw new DAOException("Can't create FullTextQuery for multiple persistent classes");
		}
		
		log.debug("Creating new SearchQuerySelectStep instance");

		@SuppressWarnings("unchecked") // Suppress unchecked cast warning
		Class<T> entityClass = (Class<T>) entities.iterator().next();
		SearchQuerySelectStep<?, EntityReference, T, SearchLoadingOptionsStep, ?, ?> query = delegate.search(entityClass);
		
		log.debug("Notifying FullTextQueryCreated listeners...");

		//Notify listeners, note that we intentionally don't catch any exception from a listener
		//so that failure should just halt the entire creation operation, this is possible because 
		//the default ApplicationEventMulticaster in spring fires events serially in the same thread
		//but has the downside of where a rogue listener can block the entire application.
//		FullTextQueryAndEntityClass queryAndClass = new FullTextQueryAndEntityClass(query, entityClass);
//		eventPublisher.publishEvent(new FullTextQueryCreatedEvent(queryAndClass)); TODO

		return query;
	}

	@Override
	public <T> SearchQuerySelectStep<?, EntityReference, T, SearchLoadingOptionsStep, ?, ?> search(SearchScope<T> searchScope) {
		return delegate.search(searchScope);
	}

	@Override
	public SearchSchemaManager schemaManager(Collection<? extends Class<?>> collection) {
		return delegate.schemaManager(collection);
	}

	@Override
	public SearchWorkspace workspace(Collection<? extends Class<?>> collection) {
		return delegate.workspace(collection);
	}

	@Override
	public org.hibernate.search.mapper.orm.massindexing.MassIndexer massIndexer(Collection<? extends Class<?>> collection) {
		return delegate.massIndexer(collection);
	}

	@Override
	public SearchIndexingPlan indexingPlan() {
		return delegate.indexingPlan();
	}

	@Override
	public EntityManager toEntityManager() {
		return delegate.toEntityManager();
	}

	@Override
	public Session toOrmSession() {
		return delegate.toOrmSession();
	}

	@Override
	public void automaticIndexingSynchronizationStrategy(AutomaticIndexingSynchronizationStrategy automaticIndexingSynchronizationStrategy) {
		delegate.automaticIndexingSynchronizationStrategy(automaticIndexingSynchronizationStrategy);
	}

	@Override
	public void indexingPlanSynchronizationStrategy(IndexingPlanSynchronizationStrategy indexingPlanSynchronizationStrategy) {
		delegate.indexingPlanSynchronizationStrategy(indexingPlanSynchronizationStrategy);
	}

	@Override
	public void indexingPlanFilter(SearchIndexingPlanFilter searchIndexingPlanFilter) {
		delegate.indexingPlanFilter(searchIndexingPlanFilter);
	}

	@Override
	public <T> SearchScope<T> scope(Collection<? extends Class<? extends T>> collection) {
		return delegate.scope(collection);
	}

	@Override
	public <T> SearchScope<T> scope(Class<T> aClass, Collection<String> collection) {
		return delegate.scope(aClass, collection);
	}
}
