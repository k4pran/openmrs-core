package org.openmrs.api.db.jpa.repository;

import org.openmrs.OrderSetAttributeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderSetAttributeTypeRepository extends JpaRepository<OrderSetAttributeType, Integer> {

	OrderSetAttributeType findByName(String name);
	OrderSetAttributeType findByUuid(String uuid);
}
