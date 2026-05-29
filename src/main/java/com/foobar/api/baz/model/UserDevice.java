package com.foobar.api.baz.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UserDevice {
    private String endpointArn;
    private String platform;
    private String environment;
}
