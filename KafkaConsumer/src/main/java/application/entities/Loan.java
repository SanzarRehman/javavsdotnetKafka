package application.entities;

import application.messages.UserContext;


import jakarta.persistence.Entity;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.util.Date;
import java.util.UUID;

@Table(name = "loans")
public class Loan implements Persistable<UUID> {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column("id")
  private UUID id;

  @Column("member_id")
  private UUID memberId;

    @Column("amount")
  private double amount;

    @Column("created_by")
  private UUID createdBy;

    @Column("created_date")
  private Date createdDate;

    @Column("language")
  private String language;

    @Column("last_updated_by")
  private Date lastUpdatedDate;

    @Column("last_updated_by")
  private UUID lastUpdatedBy;

    @Column("tenant_id")
  private UUID tenantId;

    @Column("vertical_id")
  private UUID verticalId;

    @Column("service_id")
  private String serviceId;

    @Column("is_marked_to_delete")
  private boolean isMarkedToDelete;

    @Column("version")
  private int version;


  @org.springframework.data.annotation.Transient
  Boolean newEntry;


  @Override
  public UUID getId() {
    return UUID.randomUUID();
  }

  @Override
  public boolean isNew() {
    if (this.newEntry == null) {
      return false;
    } else {
      return this.newEntry;
    }
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public void setServiceId(String serviceId) {
    this.serviceId = serviceId;
  }

  public void assignEntityDefaults(UserContext userContext) {
    this.isMarkedToDelete = false;
    this.language = userContext.language();
    this.tenantId = userContext.tenantId();
    this.serviceId = userContext.serviceId();
    this.verticalId = userContext.verticalId();
    this.createdDate = this.lastUpdatedDate = new Date();
    this.createdBy = this.lastUpdatedBy = userContext.userId();
  }

  public void setNewEntry(Boolean newEntry) {
    this.newEntry = newEntry;
  }

  public void setAmount(double amount) {
    this.amount = amount;
  }

  public void setMemberId(UUID memberId) {
    this.memberId = memberId;
  }
}
