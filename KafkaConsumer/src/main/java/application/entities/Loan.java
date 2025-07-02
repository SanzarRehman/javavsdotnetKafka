package application.entities;

import application.messages.UserContext;


import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;

@Entity
@Table(name = "loans")
public class Loan {

  @Id
  private UUID id;

  private UUID memberId;

  private double amount;

  private UUID createdBy;

  private Date createdDate;

  private String language;

  private Date lastUpdatedDate;

  private UUID lastUpdatedBy;

  private UUID tenantId;

  private UUID verticalId;

  private String serviceId;

  private boolean isMarkedToDelete;

  private int version;

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

  public void setAmount(double amount) {
    this.amount = amount;
  }

  public void setMemberId(UUID memberId) {
    this.memberId = memberId;
  }
}
