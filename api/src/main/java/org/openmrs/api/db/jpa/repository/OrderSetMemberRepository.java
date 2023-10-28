package org.openmrs.api.db.jpa.repository;

import org.openmrs.OrderSetMember;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderSetMemberRepository extends JpaRepository<OrderSetMember, Integer> {

	OrderSetMember findByUuid(String uuid);
}
