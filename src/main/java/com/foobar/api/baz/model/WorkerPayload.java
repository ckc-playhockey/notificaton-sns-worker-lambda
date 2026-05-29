package com.foobar.api.baz.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class WorkerPayload {
        String jobId;
        List<String> userIds;
        String notificationTopic;
        String notificationId;
        String url;
        boolean isSilentPush;
        String sendDateTime;
        String title;
        String body;
        Integer videoId;
}
