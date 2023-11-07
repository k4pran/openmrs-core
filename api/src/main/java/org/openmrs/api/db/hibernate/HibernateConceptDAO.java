/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.api.db.hibernate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openmrs.Concept;
import org.openmrs.ConceptAnswer;
import org.openmrs.ConceptAttribute;
import org.openmrs.ConceptAttributeType;
import org.openmrs.ConceptClass;
import org.openmrs.ConceptComplex;
import org.openmrs.ConceptDatatype;
import org.openmrs.ConceptDescription;
import org.openmrs.ConceptMap;
import org.openmrs.ConceptMapType;
import org.openmrs.ConceptName;
import org.openmrs.ConceptNameTag;
import org.openmrs.ConceptNumeric;
import org.openmrs.ConceptProposal;
import org.openmrs.ConceptReferenceTerm;
import org.openmrs.ConceptReferenceTermMap;
import org.openmrs.ConceptSearchResult;
import org.openmrs.ConceptSet;
import org.openmrs.ConceptSource;
import org.openmrs.ConceptStopWord;
import org.openmrs.Drug;
import org.openmrs.DrugIngredient;
import org.openmrs.DrugReferenceMap;
import org.openmrs.OpenmrsObject;
import org.openmrs.api.APIException;
import org.openmrs.api.ConceptService;
import org.openmrs.api.context.Context;
import org.openmrs.api.db.ConceptDAO;
import org.openmrs.api.db.DAOException;
import org.openmrs.api.db.hibernate.search.LuceneQuery;
import org.openmrs.collection.ListPart;
import org.openmrs.util.ConceptMapTypeComparator;
import org.openmrs.util.OpenmrsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Hibernate class for Concepts, Drugs, and related classes. <br>
 * <br>
 * Use the {@link ConceptService} to access these methods
 * 
 * @see ConceptService
 */
public class HibernateConceptDAO implements ConceptDAO {
	
	private static final Logger log = LoggerFactory.getLogger(HibernateConceptDAO.class);
	
	private SessionFactory sessionFactory;
	
	/**
	 * Sets the session factory
	 * 
	 * @param sessionFactory
	 */
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptComplex(java.lang.Integer)
	 */
	@Override
	public ConceptComplex getConceptComplex(Integer conceptId) {
		ConceptComplex cc;
		Session session = sessionFactory.getCurrentSession();
		Object obj = session.get(ConceptComplex.class, conceptId);
		// If Concept has already been read & cached, we may get back a Concept instead of
		// ConceptComplex.  If this happens, we need to clear the object from the cache
		// and re-fetch it as a ConceptComplex
		if (obj != null && !obj.getClass().equals(ConceptComplex.class)) {
			// remove from cache
			session.detach(obj);
			
			// session.get() did not work here, we need to perform a query to get a ConceptComplex
			CriteriaBuilder cb = session.getCriteriaBuilder();
			CriteriaQuery<ConceptComplex> query = cb.createQuery(ConceptComplex.class);
			Root<ConceptComplex> root = query.from(ConceptComplex.class);
			
			query.where(cb.equal(root.get("conceptId"), conceptId));
			
			obj = session.createQuery(query).uniqueResult();
		}
		cc = (ConceptComplex) obj;
		
		return cc;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveConcept(org.openmrs.Concept)
	 */
	@Override
	public Concept saveConcept(Concept concept) throws DAOException {
		if ((concept.getConceptId() != null) && (concept.getConceptId() > 0)) {
			// this method checks the concept_numeric, concept_derived, etc tables
			// to see if a row exists there or not.  This is needed because hibernate
			// doesn't like to insert into concept_numeric but update concept in the
			// same go.  It assumes that its either in both tables or no tables
			insertRowIntoSubclassIfNecessary(concept);
		}
		
		sessionFactory.getCurrentSession().saveOrUpdate(concept);
		return concept;
	}
	
	/**
	 * Convenience method that will check this concept for subtype values (ConceptNumeric,
	 * ConceptDerived, etc) and insert a line into that subtable if needed. This prevents a hibernate
	 * ConstraintViolationException
	 * 
	 * @param concept the concept that will be inserted
	 */
	private void insertRowIntoSubclassIfNecessary(Concept concept) {

		Session session = sessionFactory.getCurrentSession();
		// check the concept_numeric table
		if (concept instanceof ConceptNumeric) {

			CriteriaBuilder cb = session.getCriteriaBuilder();
			CriteriaQuery<Long> criteriaQuery = cb.createQuery(Long.class);
			Root<ConceptNumeric> root = criteriaQuery.from(ConceptNumeric.class);
			criteriaQuery.select(cb.literal(1L));
			criteriaQuery.where(cb.equal(root.get("conceptId"), concept.getConceptId()));
			
			List<Long> conceptIds = session.createQuery(criteriaQuery).getResultList();


			// Converting to concept numeric:  A single concept row exists, but concept numeric has not been populated yet.
			if (conceptIds.isEmpty()) {
				// we have to evict the current concept out of the session because
				// the user probably had to change the class of this object to get it
				// to now be a numeric
				// (must be done before the "insert into...")
				session.clear();
				
				//Just in case this was changed from concept_complex to numeric
				//We need to add a delete line for each concept sub class that is not concept_numeric
				deleteSubclassConcept("concept_complex", concept.getConceptId());
				
				ConceptNumeric conceptNumeric = new ConceptNumeric();
				conceptNumeric.setConceptId(concept.getConceptId());
				conceptNumeric.setAllowDecimal(false);

				session.persist(conceptNumeric);
			} else {
				// Converting from concept numeric:  The concept and concept numeric rows both exist, so we need to delete concept_numeric.
				
				// concept is changed from numeric to something else
				// hence row should be deleted from the concept_numeric
				if (!concept.isNumeric()) {
					deleteSubclassConcept("concept_numeric", concept.getConceptId());
				}
			}
		}
		// check the concept complex table
		else if (concept instanceof ConceptComplex) {
			
			CriteriaBuilder cb = session.getCriteriaBuilder();
			CriteriaQuery<Long> criteriaQuery = cb.createQuery(Long.class);
			Root<ConceptComplex> root = criteriaQuery.from(ConceptComplex.class);
			criteriaQuery.select(cb.literal(1L));
			criteriaQuery.where(cb.equal(root.get("conceptId"), concept.getConceptId()));

			List<Long> conceptIds = session.createQuery(criteriaQuery).getResultList();
			
			// Converting to concept complex:  A single concept row exists, but concept complex has not been populated yet.
			if (conceptIds.isEmpty()) {
				// we have to evict the current concept out of the session because
				// the user probably had to change the class of this object to get it
				// to now be a ConceptComplex
				// (must be done before the "insert into...")
				session.clear();
				
				//Just in case this was changed from concept_numeric to complex
				//We need to add a delete line for each concept sub class that is not concept_complex
				deleteSubclassConcept("concept_numeric", concept.getConceptId());
				
				// Add an empty row into the concept_complex table
				ConceptComplex conceptComplex = new ConceptComplex();
				conceptComplex.setConceptId(concept.getConceptId());

				session.persist(conceptComplex);
			} else {
				// Converting from concept complex:  The concept and concept complex rows both exist, so we need to delete the concept_complex row.
				// no stub insert is needed because either a concept row doesn't exist OR a concept_complex row does exist
				
				// concept is changed from complex to something else
				// hence row should be deleted from the concept_complex
				if (!concept.isComplex()) {
					deleteSubclassConcept("concept_complex", concept.getConceptId());
				}
			}
		}
	}
	
	/**
	 * Deletes a concept from a sub class table
	 * 
	 * @param tableName the sub class table name
	 * @param conceptId the concept id
	 */
	private void deleteSubclassConcept(String tableName, Integer conceptId) {
		Session session = sessionFactory.getCurrentSession();
		String delete = "DELETE FROM " + tableName + " WHERE concept_id = :conceptId";
		Query query = session.createNativeQuery(delete);
		query.setParameter("conceptId", conceptId);
		query.executeUpdate();
	}


	/**
	 * @see org.openmrs.api.db.ConceptDAO#purgeConcept(org.openmrs.Concept)
	 */
	@Override
	public void purgeConcept(Concept concept) throws DAOException {
		sessionFactory.getCurrentSession().delete(concept);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConcept(java.lang.Integer)
	 */
	@Override
	public Concept getConcept(Integer conceptId) throws DAOException {
		return (Concept) sessionFactory.getCurrentSession().get(Concept.class, conceptId);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptName(java.lang.Integer)
	 */
	@Override
	public ConceptName getConceptName(Integer conceptNameId) throws DAOException {
		return (ConceptName) sessionFactory.getCurrentSession().get(ConceptName.class, conceptNameId);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptAnswer(java.lang.Integer)
	 */
	@Override
	public ConceptAnswer getConceptAnswer(Integer conceptAnswerId) throws DAOException {
		return (ConceptAnswer) sessionFactory.getCurrentSession().get(ConceptAnswer.class, conceptAnswerId);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getAllConcepts(java.lang.String, boolean, boolean)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public List<Concept> getAllConcepts(String sortBy, boolean asc, boolean includeRetired) throws DAOException {
		
		boolean isNameField = false;
		
		try {
			Concept.class.getDeclaredField(sortBy);
		}
		catch (NoSuchFieldException e) {
			try {
				ConceptName.class.getDeclaredField(sortBy);
				isNameField = true;
			}
			catch (NoSuchFieldException e2) {
				sortBy = "conceptId";
			}
		}
		
		String hql = "";
		if (isNameField) {
			hql += "select concept";
		}
		
		hql += " from Concept as concept";
		boolean hasWhereClause = false;
		if (isNameField) {
			hasWhereClause = true;
			//This assumes every concept has a unique(avoid duplicates) fully specified name
			//which should be true for a clean concept dictionary
			hql += " left join concept.names as names where names.conceptNameType = 'FULLY_SPECIFIED'";
		}
		
		if (!includeRetired) {
			if (hasWhereClause) {
				hql += " and";
			} else {
				hql += " where";
			}
			hql += " concept.retired = false";
			
		}
		
		if (isNameField) {
			hql += " order by names." + sortBy;
		} else {
			hql += " order by concept." + sortBy;
		}
		
		hql += asc ? " asc" : " desc";
		Query query = sessionFactory.getCurrentSession().createQuery(hql);
		return query.getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveDrug(org.openmrs.Drug)
	 */
	@Override
	public Drug saveDrug(Drug drug) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(drug);
		return drug;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getDrug(java.lang.Integer)
	 */
	@Override
	public Drug getDrug(Integer drugId) throws DAOException {
		return (Drug) sessionFactory.getCurrentSession().get(Drug.class, drugId);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getDrugs(java.lang.String, org.openmrs.Concept, boolean)
	 */
	@Override
	public List<Drug> getDrugs(String drugName, Concept concept, boolean includeRetired) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Drug> query = cb.createQuery(Drug.class);
		Root<Drug> drugRoot = query.from(Drug.class);
		
		List<Predicate> predicates = new ArrayList<>();
		
		if (!includeRetired) {
			predicates.add(cb.equal(drugRoot.get("retired"), false));
		}
		
		if (concept != null) {
			predicates.add(cb.equal(drugRoot.get("concept"), concept));
		}
		
		if (drugName != null) {
			if (Context.getAdministrationService().isDatabaseStringComparisonCaseSensitive()) {
				predicates.add(cb.equal(cb.lower(drugRoot.get("name")), drugName.toLowerCase()));
			} else {
				predicates.add(cb.equal(drugRoot.get("name"), drugName));
			}
		}
		
		query.where(predicates.toArray(new Predicate[0]));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getDrugsByIngredient(org.openmrs.Concept)
	 */
	@Override
	public List<Drug> getDrugsByIngredient(Concept ingredient) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Drug> query = cb.createQuery(Drug.class);
		Root<Drug> drugRoot = query.from(Drug.class);
		Join<Drug, DrugIngredient> ingredientJoin = drugRoot.join("ingredients");
		
		Predicate rhs = cb.equal(drugRoot.get("concept"), ingredient);
		Predicate lhs = cb.equal(ingredientJoin.get("ingredient"), ingredient);
		
		query.where(cb.or(lhs, rhs));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getDrugs(java.lang.String)
	 */
	@Override
	public List<Drug> getDrugs(final String phrase) throws DAOException {
		LuceneQuery<Drug> drugQuery = newDrugQuery(phrase, true, false, Context.getLocale(), false, null, false);
		
		if (drugQuery == null) {
			return Collections.emptyList();
		}
		
		return drugQuery.list();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptClass(java.lang.Integer)
	 */
	@Override
	public ConceptClass getConceptClass(Integer i) throws DAOException {
		return (ConceptClass) sessionFactory.getCurrentSession().get(ConceptClass.class, i);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptClasses(java.lang.String)
	 */
	@Override
	public List<ConceptClass> getConceptClasses(String name) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptClass> query = cb.createQuery(ConceptClass.class);
		Root<ConceptClass> root = query.from(ConceptClass.class);
		
		if (name != null) {
			query.where(cb.equal(root.get("name"), name));
		}
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getAllConceptClasses(boolean)
	 */
	@Override
	public List<ConceptClass> getAllConceptClasses(boolean includeRetired) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptClass> query = cb.createQuery(ConceptClass.class);
		Root<ConceptClass> root = query.from(ConceptClass.class);
		
		// Minor bug - was assigning includeRetired instead of evaluating
		if (!includeRetired) {
			query.where(cb.equal(root.get("retired"), false));
		}
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveConceptClass(org.openmrs.ConceptClass)
	 */
	@Override
	public ConceptClass saveConceptClass(ConceptClass cc) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(cc);
		return cc;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#purgeConceptClass(org.openmrs.ConceptClass)
	 */
	@Override
	public void purgeConceptClass(ConceptClass cc) throws DAOException {
		sessionFactory.getCurrentSession().delete(cc);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#deleteConceptNameTag(ConceptNameTag)
	 */
	@Override
	public void deleteConceptNameTag(ConceptNameTag cnt) throws DAOException {
		sessionFactory.getCurrentSession().delete(cnt);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptDatatype(java.lang.Integer)
	 */
	@Override
	public ConceptDatatype getConceptDatatype(Integer i) {
		return (ConceptDatatype) sessionFactory.getCurrentSession().get(ConceptDatatype.class, i);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getAllConceptDatatypes(boolean)
	 */
	@Override
	public List<ConceptDatatype> getAllConceptDatatypes(boolean includeRetired) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptDatatype> query = cb.createQuery(ConceptDatatype.class);
		Root<ConceptDatatype> root = query.from(ConceptDatatype.class);
		
		if (!includeRetired) {
			query.where(cb.equal(root.get("retired"), false));
		}
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @param name the name of the ConceptDatatype
	 * @return a List of ConceptDatatype whose names start with the passed name
	 */
	public List<ConceptDatatype> getConceptDatatypes(String name) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptDatatype> query = cb.createQuery(ConceptDatatype.class);
		Root<ConceptDatatype> root = query.from(ConceptDatatype.class);
		
		if (name != null) {
			query.where(cb.like(root.get("name"), name + "%"));
		}
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptDatatypeByName(String)
	 */
	@Override
	public ConceptDatatype getConceptDatatypeByName(String name) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptDatatype> query = cb.createQuery(ConceptDatatype.class);
		Root<ConceptDatatype> root = query.from(ConceptDatatype.class);
		
		if (name != null) {
			query.where(cb.equal(root.get("name"), name));
		}
		return session.createQuery(query).uniqueResult();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveConceptDatatype(org.openmrs.ConceptDatatype)
	 */
	@Override
	public ConceptDatatype saveConceptDatatype(ConceptDatatype cd) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(cd);
		return cd;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#purgeConceptDatatype(org.openmrs.ConceptDatatype)
	 */
	@Override
	public void purgeConceptDatatype(ConceptDatatype cd) throws DAOException {
		sessionFactory.getCurrentSession().delete(cd);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptNumeric(java.lang.Integer)
	 */
	@Override
	public ConceptNumeric getConceptNumeric(Integer i) {
		Session session = sessionFactory.getCurrentSession();
		
		Object obj = session.get(ConceptNumeric.class, i); // Fetch as Concept
		
		// If the fetched object is not an instance of ConceptNumeric, re-fetch it.
		if (obj != null && !obj.getClass().equals(ConceptNumeric.class)) {
			session.detach(obj); // Detach the fetched object from the session.
			
			// Re-fetching as a ConceptNumeric
			CriteriaBuilder cb = session.getCriteriaBuilder();
			CriteriaQuery<ConceptNumeric> query = cb.createQuery(ConceptNumeric.class);
			Root<ConceptNumeric> root = query.from(ConceptNumeric.class);
			query.where(cb.equal(root.get("conceptId"), i));
			obj = session.createQuery(query).uniqueResult();
		}
		
		return (ConceptNumeric) obj;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConcepts(java.lang.String, java.util.Locale, boolean,
	 *      java.util.List, java.util.List)
	 */
	@Override
	public List<Concept> getConcepts(final String name, final Locale loc, final boolean searchOnPhrase,
	        final List<ConceptClass> classes, final List<ConceptDatatype> datatypes) throws DAOException {
		
		final Locale locale;
		if (loc == null) {
			locale = Context.getLocale();
		} else {
			locale = loc;
		}
		
		LuceneQuery<ConceptName> conceptNameQuery = newConceptNameLuceneQuery(name, !searchOnPhrase,
		    Collections.singletonList(locale), false, false, classes, null, datatypes, null, null);
		
		List<ConceptName> names = conceptNameQuery.list();
		
		return new ArrayList<>(transformNamesToConcepts(names));
	}
	
	private LinkedHashSet<Concept> transformNamesToConcepts(List<ConceptName> names) {
		LinkedHashSet<Concept> concepts = new LinkedHashSet<>();
		
		for (ConceptName name : names) {
			concepts.add(name.getConcept());
		}
		
		return concepts;
	}
	
	private String newConceptNameQuery(final String name, final boolean searchKeywords, final Set<Locale> locales,
	        final boolean searchExactLocale) {
		final String escapedName = LuceneQuery.escapeQuery(name).replace("AND", "and").replace("OR", "or").replace("NOT",
		    "not");
		final List<String> tokenizedName = tokenizeConceptName(escapedName, locales);
		
		final StringBuilder query = new StringBuilder();
		
		query.append("(concept.conceptMappings.conceptReferenceTerm.code:(").append(escapedName).append(")^0.4 OR (");
		final StringBuilder nameQuery = newNameQuery(tokenizedName, escapedName, searchKeywords);
		query.append(nameQuery);
		query.append(" localePreferred:true)^0.4 OR (");
		query.append(nameQuery);
		query.append(")^0.2)");
		
		List<String> localeQueries = new ArrayList<>();
		for (Locale locale : locales) {
			if (searchExactLocale) {
				localeQueries.add(locale.toString());
			} else {
				String localeQuery = locale.getLanguage() + "* ";
				if (!StringUtils.isBlank(locale.getCountry())) {
					localeQuery += " OR " + locale + "^2 ";
				}
				localeQueries.add(localeQuery);
			}
		}
		query.append(" locale:(");
		query.append(StringUtils.join(localeQueries, " OR "));
		query.append(")");
		query.append(" voided:false");
		
		return query.toString();
	}
	
	private StringBuilder newNameQuery(final List<String> tokenizedName, final String escapedName,
	        final boolean searchKeywords) {
		final StringBuilder query = new StringBuilder();
		query.append("(");
		if (searchKeywords) {
			//Put exact phrase higher
			query.append(" name:(\"").append(escapedName).append("\")^0.7");
			
			if (!tokenizedName.isEmpty()) {
				query.append(" OR (");
				for (String token : tokenizedName) {
					query.append(" (name:(");
					
					//Include exact
					query.append(token);
					query.append(")^0.6 OR name:(");
					
					//Include partial
					query.append(token);
					query.append("*)^0.3 OR name:(");
					
					//Include similar
					query.append(token);
					query.append("~0.8)^0.1)");
				}
				query.append(")^0.3");
			}
		} else {
			query.append(" name:\"").append(escapedName).append("\"");
		}
		query.append(")");
		return query;
	}
	
	private List<String> tokenizeConceptName(final String escapedName, final Set<Locale> locales) {
		List<String> words = new ArrayList<>(Arrays.asList(escapedName.trim().split(" ")));
		
		Set<String> stopWords = new HashSet<>();
		for (Locale locale : locales) {
			stopWords.addAll(Context.getConceptService().getConceptStopWords(locale));
		}
		
		List<String> tokenizedName = new ArrayList<>();
		
		for (String word : words) {
			word = word.trim();
			
			if (!word.isEmpty() && !stopWords.contains(word.toUpperCase())) {
				tokenizedName.add(word);
			}
		}
		
		return tokenizedName;
	}
	
	/**
	 * gets questions for the given answer concept
	 *
	 * @see org.openmrs.api.db.ConceptDAO#getConceptsByAnswer(org.openmrs.Concept)
	 */
	@Override
	public List<Concept> getConceptsByAnswer(Concept concept) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Concept> query = cb.createQuery(Concept.class);
		Root<Concept> root = query.from(Concept.class);
		Join<Concept, ConceptAnswer> conceptAnswerJoin = root.join("answers");
		
		query.where(cb.equal(conceptAnswerJoin.get("answerConcept"), concept));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getPrevConcept(org.openmrs.Concept)
	 */
	@Override
	public Concept getPrevConcept(Concept c) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Concept> query = cb.createQuery(Concept.class);
		Root<Concept> root = query.from(Concept.class);
		
		Integer i = c.getConceptId();
		
		query.where(cb.lessThan(root.get("conceptId"), i));
		query.orderBy(cb.desc(root.get("conceptId")));
		
		List<Concept> concepts = session.createQuery(query).setMaxResults(1).getResultList();
		
		if (concepts.isEmpty()) {
			return null;
		}
		
		return concepts.get(0);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getNextConcept(org.openmrs.Concept)
	 */
	@Override
	public Concept getNextConcept(Concept c) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Concept> query = cb.createQuery(Concept.class);
		Root<Concept> root = query.from(Concept.class);
		
		Integer i = c.getConceptId();
		
		query.where(cb.greaterThan(root.get("conceptId"), i));
		query.orderBy(cb.asc(root.get("conceptId")));
		
		List<Concept> concepts = session.createQuery(query).setMaxResults(1).getResultList();
		
		if (concepts.isEmpty()) {
			return null;
		}
		
		return concepts.get(0);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptsWithDrugsInFormulary()
	 */
	@Override
	public List<Concept> getConceptsWithDrugsInFormulary() {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Concept> query = cb.createQuery(Concept.class);
		Root<Drug> drugRoot = query.from(Drug.class);
		
		query.select(drugRoot.get("concept")).distinct(true);
		query.where(cb.equal(drugRoot.get("retired"), false));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#purgeDrug(org.openmrs.Drug)
	 */
	@Override
	public void purgeDrug(Drug drug) throws DAOException {
		sessionFactory.getCurrentSession().delete(drug);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveConceptProposal(org.openmrs.ConceptProposal)
	 */
	@Override
	public ConceptProposal saveConceptProposal(ConceptProposal cp) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(cp);
		return cp;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#purgeConceptProposal(org.openmrs.ConceptProposal)
	 */
	@Override
	public void purgeConceptProposal(ConceptProposal cp) throws DAOException {
		sessionFactory.getCurrentSession().delete(cp);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getAllConceptProposals(boolean)
	 */
	@Override
	public List<ConceptProposal> getAllConceptProposals(boolean includeCompleted) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptProposal> query = cb.createQuery(ConceptProposal.class);
		Root<ConceptProposal> root = query.from(ConceptProposal.class);
		
		if (!includeCompleted) {
			query.where(cb.equal(root.get("state"), OpenmrsConstants.CONCEPT_PROPOSAL_UNMAPPED));
		}
		query.orderBy(cb.asc(root.get("originalText")));
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptProposal(java.lang.Integer)
	 */
	@Override
	public ConceptProposal getConceptProposal(Integer conceptProposalId) throws DAOException {
		return (ConceptProposal) sessionFactory.getCurrentSession().get(ConceptProposal.class, conceptProposalId);
	}
	
	@Override
	public List<ConceptProposal> getConceptProposals(String text) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptProposal> query = cb.createQuery(ConceptProposal.class);
		Root<ConceptProposal> root = query.from(ConceptProposal.class);
		
		Predicate stateCondition = cb.equal(root.get("state"), OpenmrsConstants.CONCEPT_PROPOSAL_UNMAPPED);
		Predicate textCondition = cb.equal(root.get("originalText"), text);
		
		query.where(cb.and(stateCondition, textCondition));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getProposedConcepts(java.lang.String)
	 */
	@Override
	public List<Concept> getProposedConcepts(String text) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Concept> query = cb.createQuery(Concept.class);
		Root<ConceptProposal> root = query.from(ConceptProposal.class);
		
		Predicate stateNotEqual = cb.notEqual(root.get("state"), OpenmrsConstants.CONCEPT_PROPOSAL_UNMAPPED);
		Predicate originalTextEqual = cb.equal(root.get("originalText"), text);
		Predicate mappedConceptNotNull = cb.isNotNull(root.get("mappedConcept"));
		
		query.select(root.get("mappedConcept")).distinct(true);
		query.where(stateNotEqual, originalTextEqual, mappedConceptNotNull);
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptSetsByConcept(org.openmrs.Concept)
	 */
	@Override
	public List<ConceptSet> getConceptSetsByConcept(Concept concept) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptSet> query = cb.createQuery(ConceptSet.class);
		Root<ConceptSet> root = query.from(ConceptSet.class);
		
		query.where(cb.equal(root.get("conceptSet"), concept));
		query.orderBy(cb.asc(root.get("sortWeight")));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getSetsContainingConcept(org.openmrs.Concept)
	 */
	@Override
	public List<ConceptSet> getSetsContainingConcept(Concept concept) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptSet> query = cb.createQuery(ConceptSet.class);
		Root<ConceptSet> root = query.from(ConceptSet.class);
		
		query.where(cb.equal(root.get("concept"), concept));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * returns a list of n-generations of parents of a concept in a concept set
	 * 
	 * @param current
	 * @return List&lt;Concept&gt;
	 * @throws DAOException
	 */
	private List<Concept> getParents(Concept current) throws DAOException {
		List<Concept> parents = new ArrayList<>();
		if (current != null) {
			Session session = sessionFactory.getCurrentSession();
			CriteriaBuilder cb = session.getCriteriaBuilder();
			CriteriaQuery<Concept> query = cb.createQuery(Concept.class);
			Root<Concept> root = query.from(Concept.class);
			Join<Concept, ConceptSet> conceptSetJoin = root.join("conceptSets");
			
			query.where(cb.equal(conceptSetJoin.get("concept"), current));
			
			List<Concept> immedParents = session.createQuery(query).getResultList();
			for (Concept c : immedParents) {
				parents.addAll(getParents(c));
			}
			parents.add(current);
			if (log.isDebugEnabled()) {
				log.debug("parents found: ");
				for (Concept c : parents) {
					log.debug("id: {}", c.getConceptId());
				}
			}
		}
		return parents;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getLocalesOfConceptNames()
	 */
	@Override
	public Set<Locale> getLocalesOfConceptNames() {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Locale> query = cb.createQuery(Locale.class);
		Root<ConceptName> root = query.from(ConceptName.class);
		
		query.select(root.get("locale")).distinct(true);
		
		List<Locale> result = session.createQuery(query).getResultList();
		
		return new HashSet<>(result);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptNameTag(java.lang.Integer)
	 */
	@Override
	public ConceptNameTag getConceptNameTag(Integer i) {
		return (ConceptNameTag) sessionFactory.getCurrentSession().get(ConceptNameTag.class, i);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptNameTagByName(java.lang.String)
	 */
	@Override
	public ConceptNameTag getConceptNameTagByName(String name) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptNameTag> query = cb.createQuery(ConceptNameTag.class);
		Root<ConceptNameTag> root = query.from(ConceptNameTag.class);
		
		query.where(cb.equal(root.get("tag"), name));
		
		List<ConceptNameTag> conceptNameTags = session.createQuery(query).getResultList();
		if (conceptNameTags.isEmpty()) {
			return null;
		}
		
		return conceptNameTags.get(0);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getAllConceptNameTags()
	 */
	@Override
	public List<ConceptNameTag> getAllConceptNameTags() {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptNameTag> query = cb.createQuery(ConceptNameTag.class);
		Root<ConceptNameTag> root = query.from(ConceptNameTag.class);
		
		query.orderBy(cb.asc(root.get("tag")));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptSource(java.lang.Integer)
	 */
	@Override
	public ConceptSource getConceptSource(Integer conceptSourceId) {
		return (ConceptSource) sessionFactory.getCurrentSession().get(ConceptSource.class, conceptSourceId);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getAllConceptSources(boolean)
	 */
	@Override
	public List<ConceptSource> getAllConceptSources(boolean includeRetired) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptSource> query = cb.createQuery(ConceptSource.class);
		Root<ConceptSource> root = query.from(ConceptSource.class);
		
		if (!includeRetired) {
			query.where(cb.equal(root.get("retired"), false));
		}
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#deleteConceptSource(org.openmrs.ConceptSource)
	 */
	@Override
	public ConceptSource deleteConceptSource(ConceptSource cs) throws DAOException {
		sessionFactory.getCurrentSession().delete(cs);
		return cs;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveConceptSource(org.openmrs.ConceptSource)
	 */
	@Override
	public ConceptSource saveConceptSource(ConceptSource conceptSource) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(conceptSource);
		return conceptSource;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveConceptNameTag(org.openmrs.ConceptNameTag)
	 */
	@Override
	public ConceptNameTag saveConceptNameTag(ConceptNameTag nameTag) {
		if (nameTag == null) {
			return null;
		}
		
		sessionFactory.getCurrentSession().saveOrUpdate(nameTag);
		return nameTag;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getMaxConceptId()
	 */
	public Integer getMinConceptId() {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Integer> query = cb.createQuery(Integer.class);
		Root<Concept> conceptRoot = query.from(Concept.class);
		
		query.select(cb.min(conceptRoot.get("conceptId")));
		
		return session.createQuery(query).uniqueResult();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getMaxConceptId()
	 */
	@Override
	public Integer getMaxConceptId() {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Integer> query = cb.createQuery(Integer.class);
		Root<Concept> conceptRoot = query.from(Concept.class);
		
		query.select(cb.max(conceptRoot.get("conceptId")));
		
		return session.createQuery(query).uniqueResult();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#conceptIterator()
	 */
	@Override
	public Iterator<Concept> conceptIterator() {
		return new ConceptIterator();
	}
	
	/**
	 * An iterator that loops over all concepts in the dictionary one at a time
	 */
	private class ConceptIterator implements Iterator<Concept> {
		
		Concept currentConcept = null;
		
		Concept nextConcept;
		
		public ConceptIterator() {
			final int firstConceptId = getMinConceptId();
			nextConcept = getConcept(firstConceptId);
		}
		
		/**
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			return nextConcept != null;
		}
		
		/**
		 * @see java.util.Iterator#next()
		 */
		@Override
		public Concept next() {
			if (currentConcept != null) {
				sessionFactory.getCurrentSession().evict(currentConcept);
			}
			
			currentConcept = nextConcept;
			nextConcept = getNextConcept(currentConcept);
			
			return currentConcept;
		}
		
		/**
		 * @see java.util.Iterator#remove()
		 */
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}

	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptsByMapping(String, String, boolean)
	 */
	@Override
	@Deprecated
	public List<Concept> getConceptsByMapping(String code, String sourceName, boolean includeRetired) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();

		// This criteria is for ConceptMap and includes filters for code and sourceName.
		CriteriaQuery<ConceptMap> criteriaMap = cb.createQuery(ConceptMap.class); 
			createSearchConceptMapCriteria(criteriaMap, code, sourceName, includeRetired);

		// Create a new CriteriaQuery for Concept.
		CriteriaQuery<Concept> conceptCriteria = cb.createQuery(Concept.class);

		// We must correlate the root of the criteriaMap to the new conceptCriteria.
		Root<ConceptMap> conceptMapRoot = (Root<ConceptMap>) criteriaMap.getRoots().iterator().next();
		conceptCriteria.select(conceptMapRoot.get("concept")).distinct(true);

		// Apply the restrictions from the ConceptMap criteria to the Concept criteria.
		conceptCriteria.where(criteriaMap.getRestriction());

		// Execute the query and return the list of Concepts.
		return session.createQuery(conceptCriteria).getResultList();
	}


	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptIdsByMapping(String, String, boolean)
	 */
	@Override
	public List<Integer> getConceptIdsByMapping(String code, String sourceName, boolean includeRetired) {
		Session session = sessionFactory.getCurrentSession();

		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Integer> query = cb.createQuery(Integer.class);

		Root<ConceptMap> conceptRoot = createSearchConceptMapCriteria(query, code, sourceName, includeRetired);
		
		query.select(conceptRoot.get("concept").get("conceptId")).distinct(true);
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptByUuid(java.lang.String)
	 */
	@Override
	public Concept getConceptByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, Concept.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptClassByUuid(java.lang.String)
	 */
	@Override
	public ConceptClass getConceptClassByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptClass.class, uuid);
	}
	
	@Override
	public ConceptAnswer getConceptAnswerByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptAnswer.class, uuid);
	}
	
	@Override
	public ConceptName getConceptNameByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptName.class, uuid);
	}
	
	@Override
	public ConceptSet getConceptSetByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptSet.class, uuid);
	}
	
	@Override
	public ConceptSource getConceptSourceByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptSource.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptDatatypeByUuid(java.lang.String)
	 */
	@Override
	public ConceptDatatype getConceptDatatypeByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptDatatype.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptNumericByUuid(java.lang.String)
	 */
	@Override
	public ConceptNumeric getConceptNumericByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptNumeric.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptProposalByUuid(java.lang.String)
	 */
	@Override
	public ConceptProposal getConceptProposalByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptProposal.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getDrugByUuid(java.lang.String)
	 */
	@Override
	public Drug getDrugByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, Drug.class, uuid);
	}
	
	@Override
	public DrugIngredient getDrugIngredientByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, DrugIngredient.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptUuids()
	 */
	@Override
	public Map<Integer, String> getConceptUuids() {
		Map<Integer, String> ret = new HashMap<>();
		
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Object[]> query = cb.createQuery(Object[].class);
		Root<Concept> root = query.from(Concept.class);
		
		query.multiselect(root.get("conceptId"), root.get("uuid"));
		
		List<Object[]> list = session.createQuery(query).getResultList();
		for (Object[] o : list) {
			ret.put((Integer) o[0], (String) o[1]);
		}
		return ret;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptDescriptionByUuid(java.lang.String)
	 */
	@Override
	public ConceptDescription getConceptDescriptionByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptDescription.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptNameTagByUuid(java.lang.String)
	 */
	@Override
	public ConceptNameTag getConceptNameTagByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptNameTag.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptMapsBySource(ConceptSource)
	 */
	@Override
	public List<ConceptMap> getConceptMapsBySource(ConceptSource conceptSource) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptMap> query = cb.createQuery(ConceptMap.class);
		
		Root<ConceptMap> root = query.from(ConceptMap.class);
		Join<ConceptMap, ConceptReferenceTerm> conceptReferenceTermJoin = root.join("conceptReferenceTerm");
		
		query.where(cb.equal(conceptReferenceTermJoin.get("conceptSource"), conceptSource));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptSourceByName(java.lang.String)
	 */
	@Override
	public ConceptSource getConceptSourceByName(String conceptSourceName) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptSource> query = cb.createQuery(ConceptSource.class);
		Root<ConceptSource> root = query.from(ConceptSource.class);
		
		query.where(cb.equal(root.get("name"), conceptSourceName));
		
		return session.createQuery(query).uniqueResult();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptSourceByUniqueId(java.lang.String)
	 */
	@Override
	public ConceptSource getConceptSourceByUniqueId(String uniqueId) {
		if (StringUtils.isBlank(uniqueId)) {
			return null;
		}
		
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptSource> query = cb.createQuery(ConceptSource.class);
		Root<ConceptSource> root = query.from(ConceptSource.class);
		
		query.where(cb.equal(root.get("uniqueId"), uniqueId));
		
		return session.createQuery(query).uniqueResult();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptSourceByHL7Code(java.lang.String)
	 */
	@Override
	public ConceptSource getConceptSourceByHL7Code(String hl7Code) {
		if (StringUtils.isBlank(hl7Code)) {
			return null;
		}
		
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptSource> query = cb.createQuery(ConceptSource.class);
		Root<ConceptSource> root = query.from(ConceptSource.class);
		
		query.where(cb.equal(root.get("hl7Code"), hl7Code));
		
		return session.createQuery(query).uniqueResult();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getSavedConceptDatatype(org.openmrs.Concept)
	 */
	@Override
	public ConceptDatatype getSavedConceptDatatype(Concept concept) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptDatatype> query = cb.createQuery(ConceptDatatype.class);
		
		Root<Concept> conceptRoot = query.from(Concept.class);
		Join<Concept, ConceptDatatype> datatypeJoin = conceptRoot.join("datatype");
		
		query.select(datatypeJoin).where(cb.equal(conceptRoot.get("conceptId"), concept.getConceptId()));
		
		return session.createQuery(query).uniqueResult();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getSavedConceptName(org.openmrs.ConceptName)
	 */
	@Override
	public ConceptName getSavedConceptName(ConceptName conceptName) {
		sessionFactory.getCurrentSession().refresh(conceptName);
		return conceptName;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptStopWords(java.util.Locale)
	 */
	@Override
	public List<String> getConceptStopWords(Locale locale) throws DAOException {
		
		locale = (locale == null ? Context.getLocale() : locale);
		
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<String> query = cb.createQuery(String.class);
		Root<ConceptStopWord> root = query.from(ConceptStopWord.class);
		
		query.select(root.get("value"));
		query.where(cb.equal(root.get("locale"), locale));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveConceptStopWord(org.openmrs.ConceptStopWord)
	 */
	@Override
	public ConceptStopWord saveConceptStopWord(ConceptStopWord conceptStopWord) throws DAOException {
		if (conceptStopWord != null) {
			Session session = sessionFactory.getCurrentSession();
			CriteriaBuilder cb = session.getCriteriaBuilder();
			CriteriaQuery<ConceptStopWord> query = cb.createQuery(ConceptStopWord.class);
			Root<ConceptStopWord> root = query.from(ConceptStopWord.class);
			
			query.where(cb.and(cb.equal(root.get("value"), conceptStopWord.getValue()),
			    cb.equal(root.get("locale"), conceptStopWord.getLocale())));
			
			List<ConceptStopWord> stopWordList = session.createQuery(query).getResultList();
			
			if (!stopWordList.isEmpty()) {
				throw new DAOException("Duplicate ConceptStopWord Entry");
			}
			session.saveOrUpdate(conceptStopWord);
		}
		
		return conceptStopWord;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#deleteConceptStopWord(java.lang.Integer)
	 */
	@Override
	public void deleteConceptStopWord(Integer conceptStopWordId) throws DAOException {
		if (conceptStopWordId == null) {
			throw new DAOException("conceptStopWordId is null");
		}
		
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptStopWord> query = cb.createQuery(ConceptStopWord.class);
		Root<ConceptStopWord> root = query.from(ConceptStopWord.class);
		query.where(cb.equal(root.get("conceptStopWordId"), conceptStopWordId));
		
		ConceptStopWord csw = session.createQuery(query).uniqueResult();
		if (csw == null) {
			throw new DAOException("Concept Stop Word not found or already deleted");
		}
		session.delete(csw);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getAllConceptStopWords()
	 */
	@Override
	public List<ConceptStopWord> getAllConceptStopWords() {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptStopWord> query = cb.createQuery(ConceptStopWord.class);
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see ConceptService#getCountOfDrugs(String, Concept, boolean, boolean, boolean)
	 */
	@Override
	public Long getCountOfDrugs(String drugName, Concept concept, boolean searchKeywords, boolean searchDrugConceptNames,
	        boolean includeRetired) throws DAOException {
		LuceneQuery<Drug> drugsQuery = newDrugQuery(drugName, searchKeywords, searchDrugConceptNames, Context.getLocale(),
		    false, concept, includeRetired);
		
		if (drugsQuery == null) {
			return 0L;
		}
		
		return drugsQuery.resultSize();
	}
	
	/**
	 * <strong>Should</strong> return a drug if either the drug name or concept name matches the phase
	 * not both <strong>Should</strong> return distinct drugs <strong>Should</strong> return a drug, if
	 * phrase match concept_name No need to match both concept_name and drug_name
	 * <strong>Should</strong> return drug when phrase match drug_name even searchDrugConceptNames is
	 * false <strong>Should</strong> return a drug if phrase match drug_name No need to match both
	 * concept_name and drug_name
	 */
	@Override
	public List<Drug> getDrugs(String drugName, Concept concept, boolean searchKeywords, boolean searchDrugConceptNames,
	        boolean includeRetired, Integer start, Integer length) throws DAOException {
		LuceneQuery<Drug> drugsQuery = newDrugQuery(drugName, searchKeywords, searchDrugConceptNames, Context.getLocale(),
		    false, concept, includeRetired);
		
		if (drugsQuery == null) {
			return Collections.emptyList();
		}
		
		return drugsQuery.listPart(start, length).getList();
	}
	
	private LuceneQuery<Drug> newDrugQuery(String drugName, boolean searchKeywords, boolean searchDrugConceptNames,
	        Locale locale, boolean exactLocale, Concept concept, boolean includeRetired) {
		if (StringUtils.isBlank(drugName) && concept == null) {
			return null;
		}
		if (locale == null) {
			locale = Context.getLocale();
		}
		
		StringBuilder query = new StringBuilder();
		if (!StringUtils.isBlank(drugName)) {
			String escapedName = LuceneQuery.escapeQuery(drugName);
			List<String> tokenizedName = Arrays.asList(escapedName.trim().split("\\+"));
			query.append("(");
			query.append(newNameQuery(tokenizedName, escapedName, searchKeywords));
			query.append(")^0.3 OR drugReferenceMaps.conceptReferenceTerm.code:(\"").append(escapedName).append("\")^0.6");
		}
		
		if (concept != null) {
			query.append(" OR concept.conceptId:(").append(concept.getConceptId()).append(")^0.1");
		} else if (searchDrugConceptNames) {
			LuceneQuery<ConceptName> conceptNameQuery = newConceptNameLuceneQuery(drugName, searchKeywords,
			    Collections.singletonList(locale), exactLocale, includeRetired, null, null, null, null, null);
			List<Object[]> conceptIds = conceptNameQuery.listProjection("concept.conceptId");
			if (!conceptIds.isEmpty()) {
				CollectionUtils.transform(conceptIds, input -> ((Object[]) input)[0].toString());
				//The default Lucene clauses limit is 1024. We arbitrarily chose to use 512 here as it does not make sense to return more hits by concept name anyway.
				int maxSize = (conceptIds.size() < 512) ? conceptIds.size() : 512;
				query.append(" OR concept.conceptId:(").append(StringUtils.join(conceptIds.subList(0, maxSize), " OR "))
				        .append(")^0.1");
			}
		}
		
		LuceneQuery<Drug> drugsQuery = LuceneQuery.newQuery(Drug.class, sessionFactory.getCurrentSession(),
		    query.toString());
		if (!includeRetired) {
			drugsQuery.include("retired", false);
		}
		return drugsQuery;
	}
	
	/**
	 * @see ConceptDAO#getConcepts(String, List, boolean, List, List, List, List, Concept, Integer,
	 *      Integer)
	 */
	@Override
	public List<ConceptSearchResult> getConcepts(final String phrase, final List<Locale> locales,
	        final boolean includeRetired, final List<ConceptClass> requireClasses, final List<ConceptClass> excludeClasses,
	        final List<ConceptDatatype> requireDatatypes, final List<ConceptDatatype> excludeDatatypes,
	        final Concept answersToConcept, final Integer start, final Integer size) throws DAOException {
		
		LuceneQuery<ConceptName> query = newConceptNameLuceneQuery(phrase, true, locales, false, includeRetired,
		    requireClasses, excludeClasses, requireDatatypes, excludeDatatypes, answersToConcept);
		
		ListPart<ConceptName> names = query.listPart(start, size);
		
		List<ConceptSearchResult> results = new ArrayList<>();
		
		for (ConceptName name : names.getList()) {
			results.add(new ConceptSearchResult(phrase, name.getConcept(), name));
		}
		
		return results;
	}
	
	@Override
	public Integer getCountOfConcepts(final String phrase, List<Locale> locales, boolean includeRetired,
	        List<ConceptClass> requireClasses, List<ConceptClass> excludeClasses, List<ConceptDatatype> requireDatatypes,
	        List<ConceptDatatype> excludeDatatypes, Concept answersToConcept) throws DAOException {
		
		LuceneQuery<ConceptName> query = newConceptNameLuceneQuery(phrase, true, locales, false, includeRetired,
		    requireClasses, excludeClasses, requireDatatypes, excludeDatatypes, answersToConcept);
		
		Long size = query.resultSize();
		return size.intValue();
	}
	
	private LuceneQuery<ConceptName> newConceptNameLuceneQuery(final String phrase, boolean searchKeywords,
	        List<Locale> locales, boolean searchExactLocale, boolean includeRetired, List<ConceptClass> requireClasses,
	        List<ConceptClass> excludeClasses, List<ConceptDatatype> requireDatatypes,
	        List<ConceptDatatype> excludeDatatypes, Concept answersToConcept) {
		final StringBuilder query = new StringBuilder();
		
		if (!StringUtils.isBlank(phrase)) {
			final Set<Locale> searchLocales;
			
			if (locales == null) {
				searchLocales = new HashSet<>(Collections.singletonList(Context.getLocale()));
			} else {
				searchLocales = new HashSet<>(locales);
			}
			
			query.append(newConceptNameQuery(phrase, searchKeywords, searchLocales, searchExactLocale));
		}
		
		LuceneQuery<ConceptName> luceneQuery = LuceneQuery
		        .newQuery(ConceptName.class, sessionFactory.getCurrentSession(), query.toString())
		        .include("concept.conceptClass.conceptClassId", transformToIds(requireClasses))
		        .exclude("concept.conceptClass.conceptClassId", transformToIds(excludeClasses))
		        .include("concept.datatype.conceptDatatypeId", transformToIds(requireDatatypes))
		        .exclude("concept.datatype.conceptDatatypeId", transformToIds(excludeDatatypes));
		
		if (answersToConcept != null) {
			Collection<ConceptAnswer> answers = answersToConcept.getAnswers(false);
			
			if (answers != null && !answers.isEmpty()) {
				List<Integer> ids = new ArrayList<>();
				for (ConceptAnswer conceptAnswer : answersToConcept.getAnswers(false)) {
					ids.add(conceptAnswer.getAnswerConcept().getId());
				}
				luceneQuery.include("concept.conceptId", ids.toArray(new Object[0]));
			}
		}
		
		if (!includeRetired) {
			luceneQuery.include("concept.retired", false);
		}
		
		luceneQuery.skipSame("concept.conceptId");
		
		return luceneQuery;
	}
	
	private String[] transformToIds(final List<? extends OpenmrsObject> items) {
		if (items == null || items.isEmpty()) {
			return new String[0];
		}
		
		String[] ids = new String[items.size()];
		for (int i = 0; i < items.size(); i++) {
			ids[i] = items.get(i).getId().toString();
		}
		return ids;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptMapTypes(boolean, boolean)
	 */
	@Override
	public List<ConceptMapType> getConceptMapTypes(boolean includeRetired, boolean includeHidden) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptMapType> query = cb.createQuery(ConceptMapType.class);
		Root<ConceptMapType> root = query.from(ConceptMapType.class);

		List<Predicate> predicates = new ArrayList<>();
		if (!includeRetired) {
			predicates.add(cb.equal(root.get("retired"), false));
		}
		if (!includeHidden) {
			predicates.add(cb.equal(root.get("isHidden"), false));
		}

		query.where(predicates.toArray(new Predicate[0]));

		List<ConceptMapType> conceptMapTypes = session.createQuery(query).getResultList();
		conceptMapTypes.sort(new ConceptMapTypeComparator());

		return conceptMapTypes;
	}

	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptMapType(java.lang.Integer)
	 */
	@Override
	public ConceptMapType getConceptMapType(Integer conceptMapTypeId) throws DAOException {
		return (ConceptMapType) sessionFactory.getCurrentSession().get(ConceptMapType.class, conceptMapTypeId);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptMapTypeByUuid(java.lang.String)
	 */
	@Override
	public ConceptMapType getConceptMapTypeByUuid(String uuid) throws DAOException {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptMapType.class, uuid);
	}

	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptMapTypeByName(java.lang.String)
	 */
	@Override
	public ConceptMapType getConceptMapTypeByName(String name) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptMapType> query = cb.createQuery(ConceptMapType.class);
		Root<ConceptMapType> root = query.from(ConceptMapType.class);
		
		query.where(cb.like(cb.lower(root.get("name")), name.toLowerCase()));
		
		return session.createQuery(query).uniqueResult();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveConceptMapType(org.openmrs.ConceptMapType)
	 */
	@Override
	public ConceptMapType saveConceptMapType(ConceptMapType conceptMapType) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(conceptMapType);
		return conceptMapType;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#deleteConceptMapType(org.openmrs.ConceptMapType)
	 */
	@Override
	public void deleteConceptMapType(ConceptMapType conceptMapType) throws DAOException {
		sessionFactory.getCurrentSession().delete(conceptMapType);
	}

	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptReferenceTerms(boolean)
	 */
	@Override
	public List<ConceptReferenceTerm> getConceptReferenceTerms(boolean includeRetired) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptReferenceTerm> query = cb.createQuery(ConceptReferenceTerm.class);
		Root<ConceptReferenceTerm> root = query.from(ConceptReferenceTerm.class);
		
		if (!includeRetired) {
			query.where(cb.equal(root.get("retired"), false));
		}
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptReferenceTerm(java.lang.Integer)
	 */
	@Override
	public ConceptReferenceTerm getConceptReferenceTerm(Integer conceptReferenceTermId) throws DAOException {
		return (ConceptReferenceTerm) sessionFactory.getCurrentSession().get(ConceptReferenceTerm.class,
		    conceptReferenceTermId);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptReferenceTermByUuid(java.lang.String)
	 */
	@Override
	public ConceptReferenceTerm getConceptReferenceTermByUuid(String uuid) throws DAOException {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptReferenceTerm.class, uuid);
	}

	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptReferenceTermsBySource(ConceptSource)
	 */
	@Override
	public List<ConceptReferenceTerm> getConceptReferenceTermsBySource(ConceptSource conceptSource) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptReferenceTerm> query = cb.createQuery(ConceptReferenceTerm.class);
		Root<ConceptReferenceTerm> root = query.from(ConceptReferenceTerm.class);

		query.where(cb.equal(root.get("conceptSource"), conceptSource));
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptReferenceTermByName(java.lang.String,
	 *      org.openmrs.ConceptSource)
	 */
	@Override
	public ConceptReferenceTerm getConceptReferenceTermByName(String name, ConceptSource conceptSource) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptReferenceTerm> query = cb.createQuery(ConceptReferenceTerm.class);
		Root<ConceptReferenceTerm> root = query.from(ConceptReferenceTerm.class);

		Predicate namePredicate = cb.like(cb.lower(root.get("name")), name.toLowerCase());
		Predicate sourcePredicate = cb.equal(root.get("conceptSource"), conceptSource);

		query.where(cb.and(namePredicate, sourcePredicate));

		List<ConceptReferenceTerm> terms = session.createQuery(query).getResultList();
		if (terms.isEmpty()) {
			return null;
		} else if (terms.size() > 1) {
			throw new APIException("ConceptReferenceTerm.foundMultipleTermsWithNameInSource",
				new Object[]{name, conceptSource.getName()});
		}
		return terms.get(0);
	}


	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptReferenceTermByCode(java.lang.String,
	 *      org.openmrs.ConceptSource)
	 */
	@Override
	public ConceptReferenceTerm getConceptReferenceTermByCode(String code, ConceptSource conceptSource) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptReferenceTerm> query = cb.createQuery(ConceptReferenceTerm.class);
		Root<ConceptReferenceTerm> root = query.from(ConceptReferenceTerm.class);

		Predicate codePredicate = cb.equal(root.get("code"), code);
		Predicate sourcePredicate = cb.equal(root.get("conceptSource"), conceptSource);

		query.where(cb.and(codePredicate, sourcePredicate));

		List<ConceptReferenceTerm> terms = session.createQuery(query).getResultList();

		if (terms.isEmpty()) {
			return null;
		} else if (terms.size() > 1) {
			throw new APIException("ConceptReferenceTerm.foundMultipleTermsWithCodeInSource",
				new Object[] { code, conceptSource.getName() });
		}
		return terms.get(0);
	}


	/**
	 * @see org.openmrs.api.db.ConceptDAO#saveConceptReferenceTerm(org.openmrs.ConceptReferenceTerm)
	 */
	@Override
	public ConceptReferenceTerm saveConceptReferenceTerm(ConceptReferenceTerm conceptReferenceTerm) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(conceptReferenceTerm);
		return conceptReferenceTerm;
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#deleteConceptReferenceTerm(org.openmrs.ConceptReferenceTerm)
	 */
	@Override
	public void deleteConceptReferenceTerm(ConceptReferenceTerm conceptReferenceTerm) throws DAOException {
		sessionFactory.getCurrentSession().delete(conceptReferenceTerm);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getCountOfConceptReferenceTerms(String, ConceptSource,
	 *      boolean)
	 */
	@Override
	public Long getCountOfConceptReferenceTerms(String query, ConceptSource conceptSource, boolean includeRetired)
		throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();

		CriteriaQuery<Long> countCriteria = cb.createQuery(Long.class);
		Root<ConceptReferenceTerm> root = countCriteria.from(ConceptReferenceTerm.class);

		countCriteria.select(cb.count(root));

		// Assuming createConceptReferenceTermCriteria returns a CriteriaQuery now
		CriteriaQuery<ConceptReferenceTerm> criteria = createConceptReferenceTermCriteria(query, conceptSource, includeRetired);
		countCriteria.where(criteria.getRestriction());

		return session.createQuery(countCriteria).getSingleResult();
	}

	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptReferenceTerms(String, ConceptSource, Integer,
	 *      Integer, boolean)
	 */
	@Override
	public List<ConceptReferenceTerm> getConceptReferenceTerms(String query, ConceptSource conceptSource, Integer start,
															   Integer length, boolean includeRetired) throws APIException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();

		CriteriaQuery<ConceptReferenceTerm> criteriaQuery = createConceptReferenceTermCriteria(query, conceptSource, includeRetired);
		TypedQuery<ConceptReferenceTerm> typedQuery = session.createQuery(criteriaQuery);

		if (start != null) {
			typedQuery.setFirstResult(start);
		}
		if (length != null && length > 0) {
			typedQuery.setMaxResults(length);
		}

		return typedQuery.getResultList();
	}

	
	/**
	 * @param query
	 * @param includeRetired
	 * @return
	 */
	private CriteriaQuery<ConceptReferenceTerm> createConceptReferenceTermCriteria(String query, ConceptSource conceptSource, boolean includeRetired) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptReferenceTerm> criteriaQuery = cb.createQuery(ConceptReferenceTerm.class);
		Root<ConceptReferenceTerm> root = criteriaQuery.from(ConceptReferenceTerm.class);

		List<Predicate> predicates = new ArrayList<>();

		if (conceptSource != null) {
			predicates.add(cb.equal(root.get("conceptSource"), conceptSource));
		}
		if (!includeRetired) {
			predicates.add(cb.equal(root.get("retired"), false));
		}
		if (query != null) {
			Predicate namePredicate = cb.like(cb.lower(root.get("name")), "%" + query.toLowerCase() + "%");
			Predicate codePredicate = cb.like(cb.lower(root.get("code")), "%" + query.toLowerCase() + "%");
			predicates.add(cb.or(namePredicate, codePredicate));
		}

		criteriaQuery.where(predicates.toArray(new Predicate[0]));
		return criteriaQuery;
	}

	/**
	 * @see org.openmrs.api.db.ConceptDAO#getReferenceTermMappingsTo(ConceptReferenceTerm)
	 */
	@Override
	public List<ConceptReferenceTermMap> getReferenceTermMappingsTo(ConceptReferenceTerm term) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptReferenceTermMap> query = cb.createQuery(ConceptReferenceTermMap.class);
		Root<ConceptReferenceTermMap> root = query.from(ConceptReferenceTermMap.class);

		query.where(cb.equal(root.get("termB"), term));

		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#isConceptReferenceTermInUse(org.openmrs.ConceptReferenceTerm)
	 */
	@Override
	public boolean isConceptReferenceTermInUse(ConceptReferenceTerm term) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();

		// Check in ConceptMap table
		CriteriaQuery<Long> conceptMapQuery = cb.createQuery(Long.class);
		Root<ConceptMap> conceptMapRoot = conceptMapQuery.from(ConceptMap.class);
		conceptMapQuery.select(cb.count(conceptMapRoot));
		conceptMapQuery.where(cb.equal(conceptMapRoot.get("conceptReferenceTerm"), term));

		Long conceptMapCount = session.createQuery(conceptMapQuery).uniqueResult();
		if (conceptMapCount > 0) {
			return true;
		}

		// Check in ConceptReferenceTermMap table
		CriteriaQuery<Long> conceptReferenceTermMapQuery = cb.createQuery(Long.class);
		Root<ConceptReferenceTermMap> conceptReferenceTermMapRoot = 
			conceptReferenceTermMapQuery.from(ConceptReferenceTermMap.class);
		conceptReferenceTermMapQuery.select(cb.count(conceptReferenceTermMapRoot));
		conceptReferenceTermMapQuery.where(cb.equal(conceptReferenceTermMapRoot.get("termB"), term));

		Long conceptReferenceTermMapCount = session.createQuery(conceptReferenceTermMapQuery).uniqueResult();
		return conceptReferenceTermMapCount > 0;
	}

	/**
	 * @see org.openmrs.api.db.ConceptDAO#isConceptMapTypeInUse(org.openmrs.ConceptMapType)
	 */
	@Override
	public boolean isConceptMapTypeInUse(ConceptMapType mapType) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();

		// Check in ConceptMap table
		CriteriaQuery<Long> conceptQuery = cb.createQuery(Long.class);
		Root<ConceptMap> conceptRoot = conceptQuery.from(ConceptMap.class);
		conceptQuery.select(cb.count(conceptRoot));
		conceptQuery.where(cb.equal(conceptRoot.get("conceptMapType"), mapType));

		Long conceptCount = session.createQuery(conceptQuery).uniqueResult();
		if (conceptCount > 0) {
			return true;
		}

		// Check in ConceptReferenceTermMap table
		CriteriaQuery<Long> conceptReferenceTermMapQuery = cb.createQuery(Long.class);
		Root<ConceptReferenceTermMap> conceptReferenceTermMapRoot = conceptReferenceTermMapQuery.from(ConceptReferenceTermMap.class);
		conceptReferenceTermMapQuery.select(cb.count(conceptReferenceTermMapRoot));
		conceptReferenceTermMapQuery.where(cb.equal(conceptReferenceTermMapRoot.get("conceptMapType"), mapType));

		Long conceptReferenceTermMapCount = session.createQuery(conceptReferenceTermMapQuery).uniqueResult();
		return conceptReferenceTermMapCount > 0;
	}

	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptsByName(java.lang.String, java.util.Locale,
	 *      java.lang.Boolean)
	 */
	@Override
	public List<Concept> getConceptsByName(final String name, final Locale locale, final Boolean exactLocale) {
		
		List<Locale> locales = new ArrayList<>();
		if (locale == null) {
			locales.add(Context.getLocale());
		} else {
			locales.add(locale);
		}
		
		boolean searchExactLocale = (exactLocale == null) ? false : exactLocale;
		
		LuceneQuery<ConceptName> conceptNameQuery = newConceptNameLuceneQuery(name, true, locales, searchExactLocale, false,
		    null, null, null, null, null);
		
		List<ConceptName> names = conceptNameQuery.list();
		
		return new ArrayList<>(transformNamesToConcepts(names));
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptByName(java.lang.String)
	 */
	@Override
	public Concept getConceptByName(final String name) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptName> query = cb.createQuery(ConceptName.class);
		Root<ConceptName> root = query.from(ConceptName.class);
		Join<ConceptName, Concept> conceptJoin = root.join("concept");

		Locale locale = Context.getLocale();
		Locale language = new Locale(locale.getLanguage() + "%");
		List<Predicate> predicates = new ArrayList<>();

		predicates.add(cb.or(cb.equal(root.get("locale"), locale), cb.like(root.get("locale").as(String.class), language.toString())));
		if (Context.getAdministrationService().isDatabaseStringComparisonCaseSensitive()) {
			predicates.add(cb.like(cb.lower(root.get("name")), name.toLowerCase()));
		} else {
			predicates.add(cb.equal(root.get("name"), name));
		}
		predicates.add(cb.equal(root.get("voided"), false));
		predicates.add(cb.equal(conceptJoin.get("retired"), false));

		query.where(predicates.toArray(new Predicate[0]));

		List<ConceptName> list = session.createQuery(query).getResultList();
		LinkedHashSet<Concept> concepts = transformNamesToConcepts(list); // Assuming you have the method transformNamesToConcepts

		if (concepts.size() == 1) {
			return concepts.iterator().next();
		} else if (list.isEmpty()) {
			log.warn("No concept found for '" + name + "'");
		} else {
			log.warn("Multiple concepts found for '" + name + "'");

			for (Concept concept : concepts) {
				for (ConceptName conceptName : concept.getNames(locale)) {
					if (conceptName.getName().equalsIgnoreCase(name)) {
						return concept;
					}
				}
				for (ConceptName indexTerm : concept.getIndexTermsForLocale(locale)) {
					if (indexTerm.getName().equalsIgnoreCase(name)) {
						return concept;
					}
				}
			}
		}

		return null;
	}

	/**
	 * @see org.openmrs.api.db.ConceptDAO#getDefaultConceptMapType()
	 */
	@Override
	public ConceptMapType getDefaultConceptMapType() throws DAOException {
		FlushMode previousFlushMode = sessionFactory.getCurrentSession().getHibernateFlushMode();
		sessionFactory.getCurrentSession().setHibernateFlushMode(FlushMode.MANUAL);
		try {
			//Defaults to same-as if the gp is not set.
			String defaultConceptMapType = Context.getAdministrationService()
			        .getGlobalProperty(OpenmrsConstants.GP_DEFAULT_CONCEPT_MAP_TYPE);
			if (defaultConceptMapType == null) {
				throw new DAOException("The default concept map type is not set. You need to set the '"
				        + OpenmrsConstants.GP_DEFAULT_CONCEPT_MAP_TYPE + "' global property.");
			}
			
			ConceptMapType conceptMapType = getConceptMapTypeByName(defaultConceptMapType);
			if (conceptMapType == null) {
				throw new DAOException("The default concept map type (name: " + defaultConceptMapType
				        + ") does not exist! You need to set the '" + OpenmrsConstants.GP_DEFAULT_CONCEPT_MAP_TYPE
				        + "' global property.");
			}
			return conceptMapType;
		}
		finally {
			sessionFactory.getCurrentSession().setHibernateFlushMode(previousFlushMode);
		}
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#isConceptNameDuplicate(org.openmrs.ConceptName)
	 */
	@Override
	public boolean isConceptNameDuplicate(ConceptName name) {
		if (name.getVoided()) {
			return false;
		}
		if (name.getConcept() != null) {
			if (name.getConcept().getRetired()) {
				return false;
			}

			//If it is not a default name of a concept, it cannot be a duplicate.
			//Note that a concept may not have a default name for the given locale, if just a short name or
			//a search term is set.
			if (!name.equals(name.getConcept().getName(name.getLocale()))) {
				return false;
			}
		}

		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptName> query = cb.createQuery(ConceptName.class);
		Root<ConceptName> root = query.from(ConceptName.class);

		List<Predicate> predicates = new ArrayList<>();

		predicates.add(cb.equal(root.get("voided"), false));
		predicates.add(cb.or(cb.equal(root.get("locale"), name.getLocale()),
			cb.equal(root.get("locale"), new Locale(name.getLocale().getLanguage()))));

		if (Context.getAdministrationService().isDatabaseStringComparisonCaseSensitive()) {
			predicates.add(cb.equal(cb.lower(root.get("name")), name.getName().toLowerCase()));
		} else {
			predicates.add(cb.equal(root.get("name"), name.getName()));
		}

		query.where(predicates.toArray(new Predicate[0]));

		List<ConceptName> candidateNames = session.createQuery(query).getResultList();

		for (ConceptName candidateName : candidateNames) {
			if (candidateName.getConcept().getRetired()) {
				continue;
			}
			if (candidateName.getConcept().equals(name.getConcept())) {
				continue;
			}
			// If it is a default name for a concept
			if (candidateName.getConcept().getName(candidateName.getLocale()).equals(candidateName)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * @see ConceptDAO#getDrugs(String, java.util.Locale, boolean, boolean)
	 */
	@Override
	public List<Drug> getDrugs(String searchPhrase, Locale locale, boolean exactLocale, boolean includeRetired) {
		LuceneQuery<Drug> drugQuery = newDrugQuery(searchPhrase, true, true, locale, exactLocale, null, includeRetired);
		
		return drugQuery.list();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getDrugsByMapping(String, ConceptSource, Collection, boolean)
	 */
	@Override
	public List<Drug> getDrugsByMapping(String code, ConceptSource conceptSource,
										Collection<ConceptMapType> withAnyOfTheseTypes, boolean includeRetired) throws DAOException {

		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Drug> criteriaQuery = cb.createQuery(Drug.class);

		CriteriaQuery<Drug> searchCriteria = createSearchDrugByMappingCriteria(code, conceptSource, includeRetired);
		Root<Drug> drugRoot = searchCriteria.from(Drug.class);

		if (!withAnyOfTheseTypes.isEmpty()) {
			Path<ConceptMapType> conceptMapTypePath = drugRoot.get("conceptMapType");
			CriteriaBuilder.In<ConceptMapType> inClause = cb.in(conceptMapTypePath);
			for (ConceptMapType mapType : withAnyOfTheseTypes) {
				inClause.value(mapType);
			}
			searchCriteria.where(cb.and(searchCriteria.getRestriction(), inClause));
		}

		TypedQuery<Drug> query = session.createQuery(searchCriteria);
		return query.getResultList();
	}

	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getDrugs
	 */
	@Override
	public Drug getDrugByMapping(String code, ConceptSource conceptSource,
								 Collection<ConceptMapType> withAnyOfTheseTypesOrOrderOfPreference) throws DAOException {

		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();

		if (withAnyOfTheseTypesOrOrderOfPreference.isEmpty()) {
			return null;
		}

		Drug drug = null;
		for (ConceptMapType conceptMapType : withAnyOfTheseTypesOrOrderOfPreference) {
			CriteriaQuery<Drug> criteriaQuery = cb.createQuery(Drug.class);
			Root<Drug> drugRoot = criteriaQuery.from(Drug.class);

			CriteriaQuery<Drug> searchCriteria = createSearchDrugByMappingCriteria(code, conceptSource, true)
				.select(drugRoot)
				.where(cb.equal(drugRoot.get("conceptMapType"), conceptMapType));

			List<Drug> drugs = session.createQuery(searchCriteria).getResultList();
			if (drugs.size() > 1) {
				throw new DAOException("There are multiple matches for the highest-priority ConceptMapType");
			} else if (drugs.size() == 1) {
				drug = drugs.get(0);
				break;
			}
		}

		return drug;
	}

	
	/**
	 * @see ConceptDAO#getAllConceptAttributeTypes()
	 */
	@Override
	public List<ConceptAttributeType> getAllConceptAttributeTypes() {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptAttributeType> query = cb.createQuery(ConceptAttributeType.class);

		return session.createQuery(query).getResultList();
	}

	
	/**
	 * @see ConceptDAO#saveConceptAttributeType(ConceptAttributeType)
	 */
	@Override
	public ConceptAttributeType saveConceptAttributeType(ConceptAttributeType conceptAttributeType) {
		sessionFactory.getCurrentSession().saveOrUpdate(conceptAttributeType);
		return conceptAttributeType;
	}
	
	/**
	 * @see ConceptDAO#getConceptAttributeType(Integer)
	 */
	@Override
	public ConceptAttributeType getConceptAttributeType(Integer id) {
		return (ConceptAttributeType) sessionFactory.getCurrentSession().get(ConceptAttributeType.class, id);
	}
	
	/**
	 * @see ConceptDAO#getConceptAttributeTypeByUuid(String)
	 */
	@Override
	public ConceptAttributeType getConceptAttributeTypeByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptAttributeType.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#deleteConceptAttributeType(org.openmrs.ConceptAttributeType)
	 */
	@Override
	public void deleteConceptAttributeType(ConceptAttributeType conceptAttributeType) {
		sessionFactory.getCurrentSession().delete(conceptAttributeType);
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptAttributeTypes(String)
	 */
	@Override
	public List<ConceptAttributeType> getConceptAttributeTypes(String name) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptAttributeType> query = cb.createQuery(ConceptAttributeType.class);
		Root<ConceptAttributeType> root = query.from(ConceptAttributeType.class);

		if (name != null) {
			query.where(cb.like(cb.lower(root.get("name")), "%" + name.toLowerCase() + "%"));
		}
		
		return session.createQuery(query).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.ConceptDAO#getConceptAttributeTypeByName(String)
	 */
	@Override
	public ConceptAttributeType getConceptAttributeTypeByName(String exactName) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<ConceptAttributeType> query = cb.createQuery(ConceptAttributeType.class);
		Root<ConceptAttributeType> root = query.from(ConceptAttributeType.class);
		
		query.where(cb.equal(root.get("name"), exactName));
		
		return session.createQuery(query).uniqueResult();
	}
	
	/**
	 * @see ConceptDAO#getConceptAttributeByUuid(String)
	 */
	@Override
	public ConceptAttribute getConceptAttributeByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, ConceptAttribute.class, uuid);
	}
	
	/**
	 * @see ConceptDAO#getConceptAttributeCount(ConceptAttributeType)
	 */
	@Override
	public long getConceptAttributeCount(ConceptAttributeType conceptAttributeType) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Long> query = cb.createQuery(Long.class);
		Root<ConceptAttribute> root = query.from(ConceptAttribute.class);

		query.select(cb.count(root));
		query.where(cb.equal(root.get("attributeType"), conceptAttributeType));
		
		return session.createQuery(query).getSingleResult();
	}


	@Override
	public List<Concept> getConceptsByClass(ConceptClass conceptClass) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Concept> query = cb.createQuery(Concept.class);
		Root<Concept> root = query.from(Concept.class);

		query.where(cb.equal(root.get("conceptClass"), conceptClass));

		return session.createQuery(query).getResultList();
	}

	private CriteriaQuery<Drug> createSearchDrugByMappingCriteria(String code, ConceptSource conceptSource, boolean includeRetired) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Drug> query = cb.createQuery(Drug.class);
		Root<Drug> drugRoot = query.from(Drug.class);
		List<Predicate> predicates = new ArrayList<>();

		// join to the drugReferenceMap table
		Join<Drug, DrugReferenceMap> mapJoin = drugRoot.join("drugReferenceMaps");
		if (code != null || conceptSource != null) {
			// join to the conceptReferenceTerm table
			Join<DrugReferenceMap, ConceptReferenceTerm> termJoin = mapJoin.join("conceptReferenceTerm");
			if (code != null) {
				// match the source code to the passed code
				predicates.add(cb.equal(termJoin.get("code"), code));
			}
			if (conceptSource != null) {
				// match the conceptSource to the passed in concept source, null accepted
				predicates.add(cb.equal(termJoin.get("conceptSource"), conceptSource));
			}
		}
		// check whether retired or not retired drugs
		if (!includeRetired) {
			predicates.add(cb.isFalse(drugRoot.get("retired")));
		}

		query.select(drugRoot).where(predicates.toArray(new Predicate[0])).distinct(true);

		return query;
	}

	private <T> Root<ConceptMap> createSearchConceptMapCriteria(CriteriaQuery<T> query, String code, String sourceName, boolean includeRetired) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		Root<ConceptMap> conceptMapRoot = query.from(ConceptMap.class);

		// Prepare a list to collect predicates
		List<Predicate> predicates = new ArrayList<>();

		//join to the conceptReferenceTerm table
		Join<ConceptMap, ConceptReferenceTerm> termJoin = conceptMapRoot.join("conceptReferenceTerm");

		// match the source code to the passed code
		if (Context.getAdministrationService().isDatabaseStringComparisonCaseSensitive()) {
			predicates.add(cb.equal(termJoin.get("code"), code));
		} else {
			predicates.add(cb.equal(cb.lower(termJoin.get("code")), code.toLowerCase()));
		}

		// join to concept reference source and match to the hl7Code or source name
		Join<ConceptReferenceTerm, ConceptSource> sourceJoin = termJoin.join("conceptSource");

		// join to concept reference source and match to the h17Code or source name
		if (Context.getAdministrationService().isDatabaseStringComparisonCaseSensitive()) {
			Predicate sourceNameCondition = cb.equal(sourceJoin.get("name"), sourceName);
			Predicate sourceHl7CodeCondition = cb.equal(sourceJoin.get("hl7Code"), sourceName);
			predicates.add(cb.or(sourceNameCondition, sourceHl7CodeCondition));
		} else {
			Predicate sourceNameCondition = cb.equal(cb.lower(sourceJoin.get("name")), sourceName.toLowerCase());
			Predicate sourceHl7CodeCondition = cb.equal(cb.lower(sourceJoin.get("hl7Code")), sourceName.toLowerCase());
			predicates.add(cb.or(sourceNameCondition, sourceHl7CodeCondition));
		}

		Join<ConceptMap, Concept> conceptJoin = conceptMapRoot.join("concept");
		if (!includeRetired) {
			// ignore retired concepts
			predicates.add(cb.equal(conceptJoin.get("retired"), false));
		}
		
		query.where(predicates.toArray(new Predicate[0]));
		
		return conceptMapRoot;
	}
}
