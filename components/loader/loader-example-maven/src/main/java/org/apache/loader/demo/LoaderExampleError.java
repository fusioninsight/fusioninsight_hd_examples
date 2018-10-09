package org.apache.loader.demo;

import org.apache.sqoop.common.ErrorCode;

public enum LoaderExampleError implements ErrorCode{
    /** The options is invalid **/
    UNAVAILABLE_SERVER_IP("The loader service is unavailable."),
    
    CREATE_JOB_FAIL("Failed to create the job."),
    
    UPDATE_JOB_FAIL("Failed to update the job."),
    
    DELETE_JOB_FAIL("Failed to delete the job."),
    ;
    
    private final String message;
    
    LoaderExampleError(String message) {
      this.message = message;
    }
    
    public String getCode() {
      return this.name();
    }

    public String getMessage() {
      return message;
    }
}
