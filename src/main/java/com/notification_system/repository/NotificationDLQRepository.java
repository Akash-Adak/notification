package com.notification_system.repository;


import com.notification_system.model.NotificationDLQEntity;
import com.notification_system.model.NotificationEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface NotificationDLQRepository extends JpaRepository<NotificationDLQEntity, UUID> {

}
