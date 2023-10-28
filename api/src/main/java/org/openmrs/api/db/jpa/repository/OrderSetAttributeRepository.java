package org.openmrs.api.db.jpa.repository;

import org.openmrs.OrderSetAttribute;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderSetAttributeRepository extends JpaRepository<OrderSetAttribute, Integer> {

	OrderSetAttribute findByUuid(String uuid);
}
