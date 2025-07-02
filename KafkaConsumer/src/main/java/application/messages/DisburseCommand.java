package application.messages;

import java.util.UUID;

public record DisburseCommand(double amount, UUID loanId, UUID memberId, UUID correlationId, UserContext userContext) {
}
