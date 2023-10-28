package org.openmrs.api.db.jpa.repository;

import java.util.List;

import org.openmrs.OrderSet;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderSetRepository extends JpaRepository<OrderSet, Integer> {

	List<OrderSet> findByRetired(Boolean retired);

	OrderSet findByUuid(String uuid);
}
