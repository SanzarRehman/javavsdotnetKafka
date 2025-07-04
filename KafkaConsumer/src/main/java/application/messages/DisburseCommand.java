package application.messages;


import java.util.UUID;

public record DisburseCommand(String name, UUID correlationId, UserContext userContext) {
}
