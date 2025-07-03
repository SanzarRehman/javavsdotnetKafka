package application.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DisburseCommand {
    
    @JsonProperty("user_context")
    private UserContext userContext;
    
    public UserContext getUserContext() { return userContext; }
    public void setUserContext(UserContext userContext) { this.userContext = userContext; }
    
    public static class UserContext {
        @JsonProperty("service_id")
        private String serviceId;
        
        public String getServiceId() { return serviceId; }
        public void setServiceId(String serviceId) { this.serviceId = serviceId; }
    }
}
