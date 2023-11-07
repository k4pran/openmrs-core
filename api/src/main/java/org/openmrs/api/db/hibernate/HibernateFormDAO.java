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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openmrs.Concept;
import org.openmrs.EncounterType;
import org.openmrs.Field;
import org.openmrs.FieldAnswer;
import org.openmrs.FieldType;
import org.openmrs.Form;
import org.openmrs.FormField;
import org.openmrs.FormResource;
import org.openmrs.api.APIException;
import org.openmrs.api.db.DAOException;
import org.openmrs.api.db.FormDAO;
import org.openmrs.util.OpenmrsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hibernate-specific Form-related functions. This class should not be used directly. All calls
 * should go through the {@link org.openmrs.api.FormService} methods.
 *
 * @see org.openmrs.api.db.FormDAO
 * @see org.openmrs.api.FormService
 */
public class HibernateFormDAO implements FormDAO {
	
	private static final Logger log = LoggerFactory.getLogger(HibernateFormDAO.class);
	
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
	 * Returns the form object originally passed in, which will have been persisted.
	 *
	 * @see org.openmrs.api.FormService#createForm(org.openmrs.Form)
	 */
	@Override
	public Form saveForm(Form form) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(form);
		return form;
	}
	
	/**
	 * @see org.openmrs.api.FormService#duplicateForm(org.openmrs.Form)
	 */
	@Override
	public Form duplicateForm(Form form) throws DAOException {
		return (Form) sessionFactory.getCurrentSession().merge(form);
	}
	
	/**
	 * @see org.openmrs.api.FormService#deleteForm(org.openmrs.Form)
	 */
	@Override
	public void deleteForm(Form form) throws DAOException {
		sessionFactory.getCurrentSession().delete(form);
	}
	
	/**
	 * @see org.openmrs.api.FormService#getForm(java.lang.Integer)
	 */
	@Override
	public Form getForm(Integer formId) throws DAOException {
		return (Form) sessionFactory.getCurrentSession().get(Form.class, formId);
	}
	
	/**
	 * @see org.openmrs.api.FormService#getFormFields(Form)
	 */
	public List<FormField> getFormFields(Form form) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<FormField> cq = cb.createQuery(FormField.class);
		Root<FormField> root = cq.from(FormField.class);

		cq.select(root).where(cb.equal(root.get("form"), form));

		return session.createQuery(cq).getResultList();
	}


	/**
	 * @see org.openmrs.api.db.FormDAO#getFields(java.lang.String)
	 */
	@Override
	
	public List<Field> getFields(String search) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Field> cq = cb.createQuery(Field.class);
		Root<Field> root = cq.from(Field.class);

		cq.select(root)
			.where(cb.like(root.get("name"), "%" + search + "%"))
			.orderBy(cb.asc(root.get("name")));

		return session.createQuery(cq).getResultList();
	}


	/**
	 * @see org.openmrs.api.FormService#getFieldsByConcept(org.openmrs.Concept)
	 */
	public List<Field> getFieldsByConcept(Concept concept) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Field> cq = cb.createQuery(Field.class);
		Root<Field> root = cq.from(Field.class);

		cq.select(root)
			.where(cb.equal(root.get("concept"), concept))
			.orderBy(cb.asc(root.get("name")));

		return session.createQuery(cq).getResultList();
	}

	/**
	 * @see org.openmrs.api.FormService#getField(java.lang.Integer)
	 * @see org.openmrs.api.db.FormDAO#getField(java.lang.Integer)
	 */
	@Override
	public Field getField(Integer fieldId) throws DAOException {
		return (Field) sessionFactory.getCurrentSession().get(Field.class, fieldId);
	}
	
	/**
	 * @see org.openmrs.api.FormService#getAllFields(boolean)
	 * @see org.openmrs.api.db.FormDAO#getAllFields(boolean)
	 */
	@Override
	public List<Field> getAllFields(boolean includeRetired) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Field> cq = cb.createQuery(Field.class);
		Root<Field> root = cq.from(Field.class);

		if (!includeRetired) {
			cq.where(cb.isFalse(root.get("retired")));
		}

		return session.createQuery(cq).getResultList();
	}


	/**
	 * @see org.openmrs.api.FormService#getFieldType(java.lang.Integer)
	 * @see org.openmrs.api.db.FormDAO#getFieldType(java.lang.Integer)
	 */
	@Override
	public FieldType getFieldType(Integer fieldTypeId) throws DAOException {
		return (FieldType) sessionFactory.getCurrentSession().get(FieldType.class, fieldTypeId);
	}
	
	/**
	 * @see org.openmrs.api.FormService#getFieldTypes()
	 * @see org.openmrs.api.db.FormDAO#getAllFieldTypes(boolean)
	 */
	@Override
	public List<FieldType> getAllFieldTypes(boolean includeRetired) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<FieldType> cq = cb.createQuery(FieldType.class);
		Root<FieldType> root = cq.from(FieldType.class);

		if (!includeRetired) {
			cq.where(cb.equal(root.get("retired"), false));
		}

		return session.createQuery(cq).getResultList();
	}


	/**
	 * @see org.openmrs.api.FormService#getFormField(java.lang.Integer)
	 * @see org.openmrs.api.db.FormDAO#getFormField(java.lang.Integer)
	 */
	@Override
	public FormField getFormField(Integer formFieldId) throws DAOException {
		return (FormField) sessionFactory.getCurrentSession().get(FormField.class, formFieldId);
	}
	
	/**
	 * @see org.openmrs.api.FormService#getFormField(org.openmrs.Form, org.openmrs.Concept,
	 *      java.util.Collection, boolean)
	 * @see org.openmrs.api.db.FormDAO#getFormField(org.openmrs.Form, org.openmrs.Concept,
	 *      java.util.Collection, boolean)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public FormField getFormField(Form form, Concept concept, Collection<FormField> ignoreFormFields, boolean force)
	        throws DAOException {
		if (form == null) {
			log.debug("form is null, no fields will be matched");
			return null;
		}
		
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<FormField> cq = cb.createQuery(FormField.class);
		Root<FormField> root = cq.from(FormField.class);

		Join<FormField, Field> fieldJoin = root.join("field");

		cq.select(root).where(cb.and(
			cb.equal(fieldJoin.get("concept"), concept),
			cb.equal(root.get("form"), form)
		));

		// get the list of all formfields with this concept for this form
		List<FormField> formFields = session.createQuery(cq).getResultList();
		
		String err = "FormField warning.  No FormField matching concept '" + concept + "' for form '" + form + "'";
		
		if (formFields.isEmpty()) {
			log.debug(err);
			return null;
		}
		
		// save the first formfield in case we're not a in a "force" situation
		FormField backupPlan = formFields.get(0);
		
		// remove the formfields we're supposed to ignore from the return list
		formFields.removeAll(ignoreFormFields);
		
		// if we ended up removing all of the formfields, check to see if we're
		// in a "force" situation
		if (formFields.isEmpty()) {
			if (!force) {
				return backupPlan;
			} else {
				log.debug(err);
				return null;
			}
		} else {
			// if formFields.size() is still greater than 0
			return formFields.get(0);
		}
	}
	
	/**
	 * @see org.openmrs.api.FormService#getForms()
	 */
	@Override
	public List<Form> getAllForms(boolean includeRetired) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Form> cq = cb.createQuery(Form.class);
		Root<Form> root = cq.from(Form.class);

		if (!includeRetired) {
			cq.where(cb.equal(root.get("retired"), false));
		}

		cq.orderBy(
			cb.asc(root.get("name")),
			cb.asc(root.get("formId"))
		);

		return session.createQuery(cq).getResultList();
	}

	/**
	 * @see org.openmrs.api.db.FormDAO#getFormsContainingConcept(org.openmrs.Concept)
	 */
	@Override
	public List<Form> getFormsContainingConcept(Concept c) throws DAOException {
		String q = "select distinct ff.form from FormField ff where ff.field.concept = :concept";
		TypedQuery<Form> query = sessionFactory.getCurrentSession().createQuery(q, Form.class);
		query.setParameter("concept", c);
		
		return query.getResultList();
	}
	
	/**
	 * @see org.openmrs.api.FormService#saveField(org.openmrs.Field)
	 * @see org.openmrs.api.db.FormDAO#saveField(org.openmrs.Field)
	 */
	@Override
	public Field saveField(Field field) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(field);
		return field;
	}
	
	/**
	 * @see org.openmrs.api.FormService#deleteField(org.openmrs.Field)
	 * @see org.openmrs.api.db.FormDAO#deleteField(org.openmrs.Field)
	 */
	@Override
	public void deleteField(Field field) throws DAOException {
		sessionFactory.getCurrentSession().delete(field);
	}
	
	/**
	 * @see org.openmrs.api.FormService#createFormField(org.openmrs.FormField)
	 */
	@Override
	public FormField saveFormField(FormField formField) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(formField);
		return formField;
	}
	
	/**
	 * @see org.openmrs.api.FormService#deleteFormField(org.openmrs.FormField)
	 * @see org.openmrs.api.db.FormDAO#deleteFormField(org.openmrs.FormField)
	 */
	@Override
	public void deleteFormField(FormField formField) throws DAOException {
		sessionFactory.getCurrentSession().delete(formField);
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getAllFormFields()
	 */
	@Override
	public List<FormField> getAllFormFields() throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<FormField> cq = cb.createQuery(FormField.class);
		Root<FormField> root = cq.from(FormField.class);
		cq.select(root);

		return session.createQuery(cq).getResultList();
	}


	/**
	 * @see org.openmrs.api.db.FormDAO#getFields(java.util.Collection, java.util.Collection,
	 *      java.util.Collection, java.util.Collection, java.util.Collection, java.lang.Boolean,
	 *      java.util.Collection, java.util.Collection, java.lang.Boolean)
	 */
	@Override
	public List<Field> getFields(Collection<Form> forms, Collection<FieldType> fieldTypes, Collection<Concept> concepts,
								 Collection<String> tableNames, Collection<String> attributeNames, Boolean selectMultiple,
								 Collection<FieldAnswer> containsAllAnswers, Collection<FieldAnswer> containsAnyAnswer, Boolean retired)
		throws DAOException {

		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Field> cq = cb.createQuery(Field.class);
		Root<Field> root = cq.from(Field.class);
		List<Predicate> predicates = new ArrayList<>();

		if (!forms.isEmpty()) {
			predicates.add(root.get("form").in(forms));
		}

		if (!fieldTypes.isEmpty()) {
			predicates.add(root.get("fieldType").in(fieldTypes));
		}

		if (!concepts.isEmpty()) {
			predicates.add(root.get("concept").in(concepts));
		}

		if (!tableNames.isEmpty()) {
			predicates.add(root.get("tableName").in(tableNames));
		}

		if (!attributeNames.isEmpty()) {
			predicates.add(root.get("attributeName").in(attributeNames));
		}

		if (selectMultiple != null) {
			predicates.add(cb.equal(root.get("selectMultiple"), selectMultiple));
		}

		if (!containsAllAnswers.isEmpty()) {
			throw new APIException("Form.getFields.error", new Object[] { "containsAllAnswers" });
		}

		if (!containsAnyAnswer.isEmpty()) {
			throw new APIException("Form.getFields.error", new Object[] { "containsAnyAnswer" });
		}

		if (retired != null) {
			predicates.add(cb.equal(root.get("retired"), retired));
		}

		cq.where(cb.and(predicates.toArray(new Predicate[0])));

		return session.createQuery(cq).getResultList();
	}


	/**
	 * @see org.openmrs.api.db.FormDAO#getForm(java.lang.String, java.lang.String)
	 */
	@Override
	public Form getForm(String name, String version) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Form> cq = cb.createQuery(Form.class);
		Root<Form> root = cq.from(Form.class);

		cq.select(root).where(cb.and(
			cb.equal(root.get("name"), name), 
			cb.equal(root.get("version"), version)
		));

		return session.createQuery(cq).uniqueResult();
	}

	/**
	 * @see org.openmrs.api.db.FormDAO#getForms(java.lang.String, java.lang.Boolean,
	 *      java.util.Collection, java.lang.Boolean, java.util.Collection, java.util.Collection,
	 *      java.util.Collection)
	 */
	@Override
	public List<Form> getForms(String partialName, Boolean published, Collection<EncounterType> encounterTypes,
	        Boolean retired, Collection<FormField> containingAnyFormField, Collection<FormField> containingAllFormFields,
	        Collection<Field> fields) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Form> cq = cb.createQuery(Form.class);
		Root<Form> formRoot = cq.from(Form.class);
		
		List<Predicate> predicates = getFormCriteria(cb, cq, formRoot, partialName, published, encounterTypes, retired, containingAnyFormField,
		    containingAllFormFields, fields);

		cq.where(predicates.toArray(new Predicate[0]));

		return session.createQuery(cq).getResultList();
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFormCount(java.lang.String, java.lang.Boolean,
	 *      java.util.Collection, java.lang.Boolean, java.util.Collection, java.util.Collection,
	 *      java.util.Collection)
	 */
	@Override
	public Integer getFormCount(String partialName, Boolean published, Collection<EncounterType> encounterTypes,
	        Boolean retired, Collection<FormField> containingAnyFormField, Collection<FormField> containingAllFormFields,
	        Collection<Field> fields) throws DAOException {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Long> cq = cb.createQuery(Long.class);
		Root<Form> formRoot = cq.from(Form.class);
		
		List<Predicate> predicates = getFormCriteria(cb, cq, formRoot, partialName, published, encounterTypes, retired, containingAnyFormField,
		    containingAllFormFields, fields);
		
		cq.select(cb.count(formRoot.get("formId"))).where(predicates.toArray(new Predicate[0]));
		
		return OpenmrsUtil.convertToInteger(session.createQuery(cq).uniqueResult());
	}
	
	/**
	 * Convenience method to create the same hibernate criteria object for both getForms and
	 * getFormCount
	 *
	 * @param partialName
	 * @param published
	 * @param encounterTypes
	 * @param retired
	 * @param containingAnyFormField
	 * @param containingAllFormFields
	 * @param fields
	 * @return
	 */
	private <T> List<Predicate> getFormCriteria(CriteriaBuilder cb, CriteriaQuery<T> cq, Root<Form> formRoot, String partialName, Boolean published, Collection<EncounterType> encounterTypes,
												Boolean retired, Collection<FormField> containingAnyFormField, Collection<FormField> containingAllFormFields,
												Collection<Field> fields) {
		List<Predicate> predicates = new ArrayList<>();

		if (StringUtils.isNotEmpty(partialName)) {
			Predicate nameStartsWith = cb.like(formRoot.get("name"), partialName + "%");
			Predicate nameContains = cb.like(formRoot.get("name"), "% " + partialName + "%");
			predicates.add(cb.or(nameStartsWith, nameContains));
		}
		if (published != null) {
			predicates.add(cb.equal(formRoot.get("published"), published));
		}
		if (!encounterTypes.isEmpty()) {
			predicates.add(formRoot.get("encounterType").in(encounterTypes));
		}
		if (retired != null) {
			predicates.add(cb.equal(formRoot.get("retired"), retired));
		}
		if (!containingAnyFormField.isEmpty()) {
			// Convert form field persistents to integers
			Set<Integer> anyFormFieldIds = new HashSet<>();
			for (FormField ff : containingAnyFormField) {
				anyFormFieldIds.add(ff.getFormFieldId());
			}

			Subquery<Integer> subquery = cq.subquery(Integer.class);
			Root<FormField> subRoot = subquery.from(FormField.class);
			subquery.select(subRoot.get("form").get("formId"))
				.where(subRoot.get("formFieldId").in(anyFormFieldIds));

			predicates.add(cb.in(formRoot.get("formId")).value(subquery));
		}
		
		if (!containingAllFormFields.isEmpty()) {
			// Convert form field persistents to integers
			Set<Integer> allFormFieldIds = new HashSet<>();
			for (FormField ff : containingAllFormFields) {
				allFormFieldIds.add(ff.getFormFieldId());
			}
			Subquery<Long> subquery = cq.subquery(Long.class);
			Root<FormField> subRoot = subquery.from(FormField.class);
			subquery.select(cb.count(subRoot.get("formFieldId")))
				.where(cb.and(
					cb.equal(subRoot.get("form").get("formId"), formRoot.get("formId")),
					subRoot.get("formFieldId").in(allFormFieldIds)
				));

			predicates.add(cb.equal(subquery, (long) containingAllFormFields.size()));
		}
		// get all forms (dupes included) that have this field on them
		if (!fields.isEmpty()) {
			Join<Form, FormField> fieldsJoin = formRoot.join("formFields");
			predicates.add(fieldsJoin.get("field").in(fields));
		}

		return predicates;
	}


	/**
	 * @see org.openmrs.api.db.FormDAO#getFieldByUuid(java.lang.String)
	 */
	@Override
	public Field getFieldByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, Field.class, uuid);
	}
	
	@Override
	public FieldAnswer getFieldAnswerByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, FieldAnswer.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFieldTypeByUuid(java.lang.String)
	 */
	@Override
	public FieldType getFieldTypeByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, FieldType.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFieldTypeByName(java.lang.String)
	 */
	@Override
	public FieldType getFieldTypeByName(String name) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<FieldType> cq = cb.createQuery(FieldType.class);
		Root<FieldType> root = cq.from(FieldType.class);

		cq.select(root).where(cb.equal(root.get("name"), name));

		return session.createQuery(cq).uniqueResult();
	}

	/**
	 * @see org.openmrs.api.db.FormDAO#getFormByUuid(java.lang.String)
	 */
	@Override
	public Form getFormByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, Form.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFormFieldByUuid(java.lang.String)
	 */
	@Override
	public FormField getFormFieldByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, FormField.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFormsByName(java.lang.String)
	 */
	@Override
	public List<Form> getFormsByName(String name) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<Form> cq = cb.createQuery(Form.class);
		Root<Form> root = cq.from(Form.class);

		Predicate namePredicate = cb.equal(root.get("name"), name);
		Predicate retiredPredicate = cb.equal(root.get("retired"), false);

		cq.select(root).where(cb.and(namePredicate, retiredPredicate))
			.orderBy(cb.desc(root.get("version")));

		return session.createQuery(cq).getResultList();
	}

	/**
	 * @see org.openmrs.api.db.FormDAO#deleteFieldType(org.openmrs.FieldType)
	 */
	@Override
	public void deleteFieldType(FieldType fieldType) throws DAOException {
		sessionFactory.getCurrentSession().delete(fieldType);
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#saveFieldType(org.openmrs.FieldType)
	 */
	@Override
	public FieldType saveFieldType(FieldType fieldType) throws DAOException {
		sessionFactory.getCurrentSession().saveOrUpdate(fieldType);
		return fieldType;
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFormFieldsByField(Field)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public List<FormField> getFormFieldsByField(Field field) {
		return sessionFactory.getCurrentSession().createQuery("from FormField f where f.field = :field").setParameter("field",
		    field).list();
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFormResource(java.lang.Integer)
	 */
	@Override
	public FormResource getFormResource(Integer formResourceId) {
		return (FormResource) sessionFactory.getCurrentSession().get(FormResource.class, formResourceId);
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFormResourceByUuid(java.lang.String)
	 */
	@Override
	public FormResource getFormResourceByUuid(String uuid) {
		return HibernateUtil.getUniqueEntityByUUID(sessionFactory, FormResource.class, uuid);
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFormResource(org.openmrs.Form, java.lang.String)
	 */
	@Override
	public FormResource getFormResource(Form form, String name) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<FormResource> cq = cb.createQuery(FormResource.class);
		Root<FormResource> root = cq.from(FormResource.class);

		cq.select(root).where(cb.and(
			cb.equal(root.get("form"), form), 
			cb.equal(root.get("name"), name)
		));

		return session.createQuery(cq).uniqueResult();
	}


	/**
	 * @see org.openmrs.api.db.FormDAO#saveFormResource(org.openmrs.FormResource)
	 */
	@Override
	public FormResource saveFormResource(FormResource formResource) {
		sessionFactory.getCurrentSession().saveOrUpdate(formResource);
		return formResource;
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#deleteFormResource(org.openmrs.FormResource)
	 */
	@Override
	public void deleteFormResource(FormResource formResource) {
		sessionFactory.getCurrentSession().delete(formResource);
	}
	
	/**
	 * @see org.openmrs.api.db.FormDAO#getFormResourcesForForm(org.openmrs.Form)
	 */
	@Override
	public Collection<FormResource> getFormResourcesForForm(Form form) {
		Session session = sessionFactory.getCurrentSession();
		CriteriaBuilder cb = session.getCriteriaBuilder();
		CriteriaQuery<FormResource> cq = cb.createQuery(FormResource.class);
		Root<FormResource> root = cq.from(FormResource.class);

		cq.select(root).where(cb.equal(root.get("form"), form));

		return session.createQuery(cq).getResultList();
	}
}
