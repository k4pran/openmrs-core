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

import static org.openmrs.ConditionClinicalStatus.ACTIVE;
import static org.openmrs.ConditionClinicalStatus.RECURRENCE;
import static org.openmrs.ConditionClinicalStatus.RELAPSE;

import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openmrs.Condition;
import org.openmrs.Encounter;
import org.openmrs.Patient;
import org.openmrs.api.APIException;
import org.openmrs.api.db.ConditionDAO;
import org.openmrs.api.db.DAOException;

/**
 * Hibernate implementation of the ConditionDAO
 *
 * @see ConditionDAO
 */
public class HibernateConditionDAO implements ConditionDAO {
	
	/**
	 * Hibernate session factory
	 */
	private SessionFactory sessionFactory;
	
	/**
	 * Set session factory
	 *
	 * @param sessionFactory
	 */
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
	
	/**
	 * Gets the condition with the specified id.
	 *
	 * @param conditionId the id to search for in the database.
	 * @return the condition associated with the id.
	 */
	@Override
	public Condition getCondition(Integer conditionId) {
		return sessionFactory.getCurrentSession().get(Condition.class, conditionId);
	}
	
	/**
	 * Gets the condition by its UUID.
	 *
	 * @param uuid the UUID to search for in the database.
	 * @return the condition associated with the UUID.
	 */
	@Override
	public Condition getConditionByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, Condition.class, uuid);
	}

	/**
	 * @see org.openmrs.api.ConditionService#getConditionsByEncounter(Encounter)
	 */
	@Override
	public List<Condition> getConditionsByEncounter(Encounter encounter) throws APIException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Condition> criteriaQuery = cb.createQuery(Condition.class);
		Root<Condition> root = criteriaQuery.from(Condition.class);

		Predicate encounterIdPredicate = cb.equal(root.get("encounter").get("encounterId"), encounter.getId());
		Predicate voidedPredicate = cb.isFalse(root.get("voided"));
		criteriaQuery.where(cb.and(encounterIdPredicate, voidedPredicate));

		criteriaQuery.orderBy(cb.desc(root.get("dateCreated")));

		return session.createQuery(criteriaQuery).list();
	}


	/**
	 * Gets all active conditions related to the specified patient.
	 *
	 * @param patient the patient whose active conditions are being queried.
	 * @return all active conditions associated with the specified patient.
	 */
	@Override
	public List<Condition> getActiveConditions(Patient patient) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Condition> criteriaQuery = cb.createQuery(Condition.class);
		Root<Condition> root = criteriaQuery.from(Condition.class);

		Predicate patientIdPredicate = cb.equal(root.get("patient").get("patientId"), patient.getId());
		Predicate activeStatusPredicate = root.get("clinicalStatus").in(ACTIVE, RECURRENCE, RELAPSE);
		Predicate voidedPredicate = cb.isFalse(root.get("voided"));

		criteriaQuery.where(cb.and(patientIdPredicate, activeStatusPredicate, voidedPredicate))
			.orderBy(cb.desc(root.get("dateCreated")));

		return session.createQuery(criteriaQuery).list();
	}

	/**
	 * @see org.openmrs.api.ConditionService#getAllConditions(Patient)
	 */
	@Override
	public List<Condition> getAllConditions(Patient patient) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Condition> criteriaQuery = cb.createQuery(Condition.class);
		Root<Condition> root = criteriaQuery.from(Condition.class);

		Predicate patientIdPredicate = cb.equal(root.get("patient").get("patientId"), patient.getId());
		Predicate voidedPredicate = cb.isFalse(root.get("voided"));

		criteriaQuery.where(cb.and(patientIdPredicate, voidedPredicate))
			.orderBy(cb.desc(root.get("dateCreated")));

		return session.createQuery(criteriaQuery).list();
	}


	/**
	 * Removes a condition from the database
	 * 
	 * @param condition the condition to be deleted
	 */
	@Override
	public void deleteCondition(Condition condition) throws DAOException {
		sessionFactory.getCurrentSession().delete(condition);
	}

	/**
	 * Saves the condition.
	 *
	 * @param condition the condition to save.
	 * @return the saved condition.
	 */
	@Override
	public Condition saveCondition(Condition condition) {
		sessionFactory.getCurrentSession().saveOrUpdate(condition);
		return condition;
	}
}
