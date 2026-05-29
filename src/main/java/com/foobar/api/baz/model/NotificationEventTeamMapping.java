package com.foobar.api.baz.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder(setterPrefix = "with")
public class NotificationEventTeamMapping {
    Integer eventId;
    List<Integer> teamIds;
}
