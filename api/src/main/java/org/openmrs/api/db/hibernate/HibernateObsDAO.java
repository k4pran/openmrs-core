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
import java.util.Date;
import java.util.List;

import javax.persistence.FlushModeType;
import javax.persistence.NoResultException;
import javax.persistence.NonUniqueResultException;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openmrs.Concept;
import org.openmrs.ConceptName;
import org.openmrs.Encounter;
import org.openmrs.Location;
import org.openmrs.Obs;
import org.openmrs.Patient;
import org.openmrs.Person;
import org.openmrs.User;
import org.openmrs.api.db.DAOException;
import org.openmrs.api.db.ObsDAO;
import org.openmrs.util.OpenmrsConstants.PERSON_TYPE;

/**
 * Hibernate specific Observation related functions This class should not be used directly. All
 * calls should go through the {@link org.openmrs.api.ObsService} methods.
 *
 * @see org.openmrs.api.db.ObsDAO
 * @see org.openmrs.api.ObsService
 */
public class HibernateObsDAO implements ObsDAO {
	
	protected SessionFactory sessionFactory;
	
	/**
	 * Set session factory that allows us to connect to the database that Hibernate knows about.
	 *
	 * @param sessionFactory
	 */
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
	
	/**
	 * @see org.openmrs.api.ObsService#deleteObs(org.openmrs.Obs)
	 */
	@Override
	public void deleteObs(Obs obs) throws DAOException {
		sessionFactory.getCurrentSession().delete(obs);
	}
	
	/**
	 * @see org.openmrs.api.ObsService#getObs(java.lang.Integer)
	 */
	@Override
	public Obs getObs(Integer obsId) throws DAOException {
		return (Obs) sessionFactory.getCurrentSession().get(Obs.class, obsId);
	}
	
	/**
	 * @see org.openmrs.api.db.ObsDAO#saveObs(org.openmrs.Obs)
	 */
	@Override
	public Obs saveObs(Obs obs) throws DAOException {
		if (obs.hasGroupMembers() && obs.getObsId() != null) {
			// hibernate has a problem updating child collections
			// if the parent object was already saved so we do it
			// explicitly here
			for (Obs member : obs.getGroupMembers()) {
				if (member.getObsId() == null) {
					saveObs(member);
				}
			}
		}
		
		sessionFactory.getCurrentSession().saveOrUpdate(obs);
		
		return obs;
	}
	
	/**
	 * @see org.openmrs.api.db.ObsDAO#getObservations(List, List, List, List, List, List, List,
	 *      Integer, Integer, Date, Date, boolean, String)
	 */
	@Override
	public List<Obs> getObservations(List<Person> whom, List<Encounter> encounters, List<Concept> questions,
									 List<Concept> answers, List<PERSON_TYPE> personTypes, List<Location> locations, List<String> sortList,
									 Integer mostRecentN, Integer obsGroupId, Date fromDate, Date toDate, boolean includeVoidedObs,
									 String accessionNumber) throws DAOException {

		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Obs> cq = cb.createQuery(Obs.class);
		Root<Obs> root = cq.from(Obs.class);

		List<Predicate> predicates = createGetObservationsPredicates(cb, root, whom, encounters, questions, answers,
			personTypes, locations, obsGroupId, null,  includeVoidedObs,
			accessionNumber, fromDate, toDate);
		cq.where(predicates.toArray(new Predicate[0]));

		List<Order> orders = createObservationSortingOrders(cb, root, sortList);
		if (!orders.isEmpty()) {
			cq.orderBy(orders);
		}

		TypedQuery<Obs> query = session.createQuery(cq);
		applyObservationResultLimitation(query, mostRecentN);

		return query.getResultList();
	}

	/**
	 * @see org.openmrs.api.db.ObsDAO#getObservationCount(List, List, List, List, List, List, Integer, Date, Date, List, boolean, String)
	 */
	@Override
	public Long getObservationCount(List<Person> whom, List<Encounter> encounters, List<Concept> questions,
									List<Concept> answers, List<PERSON_TYPE> personTypes, List<Location> locations, Integer obsGroupId,
									Date fromDate, Date toDate, List<ConceptName> valueCodedNameAnswers, boolean includeVoidedObs,
									String accessionNumber) throws DAOException {

		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Long> cq = cb.createQuery(Long.class);

		Root<Obs> root = cq.from(Obs.class);

		List<Predicate> predicates = createGetObservationsPredicates(cb, root, whom, encounters, questions, answers,
			personTypes, locations, obsGroupId,
			valueCodedNameAnswers, includeVoidedObs,
			accessionNumber, fromDate, toDate);

		cq.select(cb.count(root)).where(predicates.toArray(new Predicate[0]));

		TypedQuery<Long> query = session.createQuery(cq);
		
		return query.getSingleResult();
	}

	/**
	 * A utility method for creating a criteria based on parameters (which are optional)
	 *
	 * @param whom
	 * @param encounters
	 * @param questions
	 * @param answers
	 * @param personTypes
	 * @param locations
	 * @param sortList If a field needs to be in <i>asc</i> order, <code>" asc"</code> has to be appended to the field name. For example: <code>fieldname asc</code>
	 * @param mostRecentN
	 * @param obsGroupId
	 * @param fromDate
	 * @param toDate
	 * @param includeVoidedObs
	 * @param accessionNumber
	 * @return
	 */
	private <T> List<Predicate> createGetObservationsPredicates(CriteriaBuilder cb, Root<Obs> root,
															List<Person> whom, List<Encounter> encounters, List<Concept> questions, List<Concept> answers,
															List<PERSON_TYPE> personTypes, List<Location> locations, Integer obsGroupId, List<ConceptName> valueCodedNameAnswers,
															boolean includeVoidedObs, String accessionNumber, Date fromDate, Date toDate) {
		List<Predicate> predicates = new ArrayList<>();

		if (CollectionUtils.isNotEmpty(whom)) {
			predicates.add(root.get("person").in(whom));
		}

		if (CollectionUtils.isNotEmpty(encounters)) {
			predicates.add(root.get("encounter").in(encounters));
		}

		if (CollectionUtils.isNotEmpty(questions)) {
			predicates.add(root.get("concept").in(questions));
		}

		if (CollectionUtils.isNotEmpty(answers)) {
			predicates.add(root.get("valueCoded").in(answers));
		}

		if (CollectionUtils.isNotEmpty(personTypes)) {
			predicates.addAll(getCriteriaPersonModifier(cb, root, personTypes));
		}

		if (CollectionUtils.isNotEmpty(locations)) {
			predicates.add(root.get("location").in(locations));
		}

		if (obsGroupId != null) {
			Join<Obs, T> obsGroupJoin = root.join("obsGroup", JoinType.LEFT);
			predicates.add(cb.equal(obsGroupJoin.get("obsId"), obsGroupId));
		}
		
		if (fromDate != null) {
			predicates.add(cb.greaterThanOrEqualTo(root.get("obsDatetime"), fromDate));
		}

		if (toDate != null) {
			predicates.add(cb.lessThanOrEqualTo(root.get("obsDatetime"), toDate));
		}

		if (CollectionUtils.isNotEmpty(valueCodedNameAnswers)) {
			predicates.add(root.get("valueCodedName").in(valueCodedNameAnswers));
		}

		if (!includeVoidedObs) {
			predicates.add(cb.equal(root.get("voided"), false));
		}

		if (accessionNumber != null) {
			predicates.add(cb.equal(root.get("accessionNumber"), accessionNumber));
		}

		return predicates;
	}

	private List<Order> createObservationSortingOrders(CriteriaBuilder cb, Root<Obs> root, List<String> sortList) {
		List<Order> orders = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(sortList)) {
			for (String sort : sortList) {
				if (StringUtils.isNotEmpty(sort)) {
					// Split the sort string into field name and order direction
					String[] sortSplit = sort.split(" ");
					Path<Object> sortPath = root.get(sortSplit[0]);
					Order order = (sortSplit.length == 2 && "asc".equalsIgnoreCase(sortSplit[1]))
						? cb.asc(sortPath)
						: cb.desc(sortPath);
					orders.add(order);
				}
			}
		}
		return orders;
	}

	private void applyObservationResultLimitation(TypedQuery<Obs> query, Integer mostRecentN) {
		if (mostRecentN != null && mostRecentN > 0) {
			query.setMaxResults(mostRecentN);
		}
	}

	/**
	 * Convenience method that adds an expression to the given <code>criteria</code> according to
	 * what types of person objects is wanted
	 *
	 * @param criteria
	 * @param personType
	 * @return the given predicates (for chaining)
	 */
	private List<Predicate> getCriteriaPersonModifier(CriteriaBuilder cb, Root<Obs> root, List<PERSON_TYPE> personTypes) {
		List<Predicate> modifiers = new ArrayList<>();

		if (personTypes.contains(PERSON_TYPE.PATIENT)) {
			Subquery<Integer> patientSubquery = cb.createQuery().subquery(Integer.class);
			Root<Patient> patientRoot = patientSubquery.from(Patient.class);
			patientSubquery.select(patientRoot.get("patientId"));
			modifiers.add(cb.in(root.join("person").get("personId")).value(patientSubquery));
		}

		if (personTypes.contains(PERSON_TYPE.USER)) {
			Subquery<Integer> userSubquery = cb.createQuery().subquery(Integer.class);
			Root<User> userRoot = userSubquery.from(User.class);
			userSubquery.select(userRoot.get("userId"));
			modifiers.add(cb.in(root.join("person").get("personId")).value(userSubquery));
		}

		return modifiers;
	}


	/**
	 * @see org.openmrs.api.db.ObsDAO#getObsByUuid(java.lang.String)
	 */
	@Override
	public Obs getObsByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, Obs.class, uuid);
	}

	/**
	 * @see org.openmrs.api.db.ObsDAO#getRevisionObs(org.openmrs.Obs)
	 */
	@Override
	public Obs getRevisionObs(Obs initialObs) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Obs> cq = cb.createQuery(Obs.class);
		Root<Obs> root = cq.from(Obs.class);

		cq.where(cb.equal(root.get("previousVersion"), initialObs));

		return session.createQuery(cq).getSingleResult();
	}

	/**
	 * @see org.openmrs.api.db.ObsDAO#getSavedStatus(org.openmrs.Obs)
	 */
	@Override
	public Obs.Status getSavedStatus(Obs obs) {
		Session session = sessionFactory.getCurrentSession();
		FlushModeType flushMode = session.getFlushMode();
		session.setFlushMode(FlushModeType.COMMIT); // Equivalent to Hibernate's FlushMode.MANUAL
		try {
			Query sql = session.createQuery("select status from obs where obs_id = :obsId");
			sql.setParameter("obsId", obs.getObsId());
			Object result = sql.getSingleResult();
			return result != null ? Obs.Status.valueOf((String) result) : null;
		} catch (NoResultException e) {
			return null;
		} catch (NonUniqueResultException e) {
			throw new IllegalStateException("More than one status found for obs_id " + obs.getObsId(), e);
		} finally {
			session.setFlushMode(flushMode);
		}
	}
}
