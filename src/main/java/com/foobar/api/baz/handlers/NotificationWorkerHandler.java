package com.foobar.api.baz.handlers;

import com.amazonaws.lambda.thirdparty.com.google.gson.Gson;
import com.amazonaws.lambda.thirdparty.com.google.gson.GsonBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;

import com.foobar.api.baz.model.UserDevice;
import com.foobar.api.baz.model.WorkerPayload;
import com.foobar.api.baz.service.DatabaseService;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.model.InvalidParameterException;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class NotificationWorkerHandler implements RequestHandler<SQSEvent, Void> {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private static final int THREAD_POOL_SIZE = 5;

    private static final String USER_MAPPING_QUERY = "INSERT INTO public.user_notification_status (user_id, notification_id, read) VALUES (?, ?, ?) ON CONFLICT (user_id, notification_id) DO NOTHING;";

    private static final SnsClient SNS = SnsClient.builder()
            .region(Region.CA_CENTRAL_1)
            .build();

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        LambdaLogger logger = context.getLogger();

        for (SQSEvent.SQSMessage record : event.getRecords()) {
            try {
                WorkerPayload payload = GSON.fromJson(record.getBody(), WorkerPayload.class);
                logger.log("The payload is : " +  GSON.toJson(payload));
                logger.log("Processing batch for " + payload.getUserIds().size() + " users");

                processBatch(payload, logger);

            } catch (Exception e) {
                logger.log("ERROR in handleRequest: " + e.getMessage());
                throw new RuntimeException("Failed to process SQS message", e);
            }
        }

        return null;
    }

    private void processBatch(WorkerPayload payload, LambdaLogger logger) {

        logger.log("Is silent notification : " + payload.isSilentPush());
        if (!payload.isSilentPush()) {

            Map<String, List<UserDevice>> userDevicesMap =
                    fetchDevicesBulk(payload.getUserIds(), logger);

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            Set<String> processedEndpoints = ConcurrentHashMap.newKeySet();

            for (String userId : payload.getUserIds()) {
                executor.submit(() -> {
                    try {
                        int badgeCount = getUnreadNotificationCount(userId, logger);
                        List<UserDevice> devices = userDevicesMap.getOrDefault(userId, Collections.emptyList());

                        if (devices.isEmpty()) {
                            logger.log("No devices for user: " + userId);
                            return;
                        }

                        for (UserDevice device : devices) {
                            String endpointArn = device.getEndpointArn();
                            if (endpointArn == null || !processedEndpoints.add(endpointArn)) {
                                logger.log("Skipping null or duplicate endpoint: " + endpointArn);
                                continue;
                            }

                            logger.log(GSON.toJson(device));
                            String message = buildPlatformMessage(device.getPlatform(), device.getEnvironment(), badgeCount, payload, logger);
                            logger.log(message);
                            sendNotification(endpointArn, message, logger);

                            Thread.sleep(5);
                        }

                    } catch (Exception e) {
                        logger.log("ERROR processing user " + userId + ": " + e.getMessage());
                    }
                });
            }

            shutdownExecutor(executor, logger);

        } else {
            // Handle silent push notifications
            Map<String, List<UserDevice>> userDevicesMap =
                    fetchDevicesBulk(payload.getUserIds(), logger);

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            Set<String> processedEndpoints = ConcurrentHashMap.newKeySet();

            for (String userId : payload.getUserIds()) {
                executor.submit(() -> {
                    try {
                        int badgeCount = getUnreadNotificationCount(userId, logger);
                        logger.log("Badge count for user " + userId + ": " + badgeCount);

                        List<UserDevice> devices = userDevicesMap.getOrDefault(userId, Collections.emptyList());

                        if (devices.isEmpty()) {
                            logger.log("No devices for user: " + userId);
                            return;
                        }

                        for (UserDevice device : devices) {
                            String endpointArn = device.getEndpointArn();
                            if (endpointArn == null || !processedEndpoints.add(endpointArn)) {
                                logger.log("Skipping null or duplicate endpoint: " + endpointArn);
                                continue;
                            }

                            logger.log(GSON.toJson(device));
                            String message = buildSilentPlatformMessage(device.getPlatform(), device.getEnvironment(), badgeCount, payload, logger);
                            logger.log(message);
                            sendNotification(endpointArn, message, logger);

                            Thread.sleep(5);
                        }

                    } catch (Exception e) {
                        logger.log("ERROR processing silent notification for user " + userId + ": " + e.getMessage());
                    }
                });
            }

            shutdownExecutor(executor, logger);
        }

        if (payload.getNotificationTopic() != null && !payload.getNotificationTopic().equals("Read Notification"))
            saveNotificationStatus(payload, logger);

    }

    private void shutdownExecutor(ExecutorService executor, LambdaLogger logger) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    private Map<String, List<UserDevice>> fetchDevicesBulk(List<String> userIds, LambdaLogger logger) {

        logger.log("Inside build Device fetching..");
        if (userIds == null || userIds.isEmpty()) return Collections.emptyMap();

        Map<String, List<UserDevice>> map = new HashMap<>();

        String placeholders = userIds.stream().map(id -> "?").collect(Collectors.joining(","));

        String query = "SELECT user_id, endpoint_arn, platform, environment FROM public.user_devices " +
                "WHERE user_id IN (" + placeholders + ") AND is_active = true";

        try (Connection conn = DatabaseService.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            for (int i = 0; i < userIds.size(); i++) {
                stmt.setObject(i + 1, UUID.fromString(userIds.get(i)));
            }

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                String userId = rs.getObject("user_id", UUID.class).toString();

                UserDevice device = UserDevice.builder()
                        .endpointArn(rs.getString("endpoint_arn"))
                        .platform(rs.getString("platform").toUpperCase())
                        .environment(rs.getString("environment").toUpperCase())
                        .build();

                map.computeIfAbsent(userId, k -> new ArrayList<>()).add(device);
            }

        } catch (Exception e) {
            throw new RuntimeException("Bulk fetch failed", e);
        }

        return map;
    }

    private void sendNotification(String endpointArn, String message, LambdaLogger logger) {
        try {
            PublishRequest request = PublishRequest.builder()
                    .targetArn(endpointArn)
                    .messageStructure("json")
                    .message(message)
                    .build();

            PublishResponse response = SNS.publish(request);
            logger.log("SNS Message sent to " + endpointArn + ". MessageId: " + response.messageId());

        } catch (EndpointDisabledException | InvalidParameterException | NotFoundException e) {
            logger.log("Endpoint invalid or disabled. Deactivating ARN: " + endpointArn + " Reason: " + e.getMessage());
            deactivateEndpoint(endpointArn, logger);
        } catch (Exception e) {
            logger.log("Error sending push notification to " + endpointArn + ": " + e.getMessage());
        }
    }

    private void deactivateEndpoint(String endpointArn, LambdaLogger logger) {
        String updateQuery = "UPDATE public.user_devices " +
                "SET is_active = false, updated_at = CURRENT_TIMESTAMP " +
                "WHERE endpoint_arn = ?";
        try (Connection conn = DatabaseService.getConnection();
             PreparedStatement stmt = conn.prepareStatement(updateQuery)) {

            stmt.setString(1, endpointArn);
            int rows = stmt.executeUpdate();
            logger.log("Deactivated endpoint_arn: " + endpointArn + ", rows updated: " + rows);

        } catch (Exception e) {
            logger.log("Failed to deactivate endpoint " + endpointArn + ": " + e.getMessage());
        }
    }

    private String buildPlatformMessage(String platform, String environment, int badgeCount, WorkerPayload payload, LambdaLogger logger) {

        Map<String, Object> message = new HashMap<>();

        logger.log("Platform: " + platform);
        logger.log("Environment : " + environment);


        message.put("default", payload.getBody());

        if ("ANDROID".equalsIgnoreCase(platform)) {

            Map<String, Object> data = new HashMap<>();
            data.put("title", payload.getTitle());
            data.put("body", payload.getBody());
            data.put("badge_count", badgeCount + 1);
            data.put("notification_id", payload.getNotificationId());

            Map<String, Object> jsonBody = new HashMap<>();

            if (payload.getNotificationId() != null) {
                jsonBody.put("notification_id", payload.getNotificationId());
            }
            if (payload.getUrl() != null && !payload.getUrl().isEmpty()) {
                jsonBody.put("external_link", payload.getUrl());
            }
            if (payload.getVideoId() != null && payload.getVideoId() != 0) {
                jsonBody.put("video_id", payload.getVideoId());
            }

            data.put("jsonBody", jsonBody);

            Map<String, Object> gcm = new HashMap<>();
            gcm.put("data", data);

            message.put("GCM", GSON.toJson(gcm));

        } else if ("IOS".equalsIgnoreCase(platform)) {

            Map<String, Object> alert = new HashMap<>();
            alert.put("title", payload.getTitle());
            alert.put("body", payload.getBody());

            Map<String, Object> aps = new HashMap<>();
            aps.put("alert", alert);
            aps.put("sound", "default");
            aps.put("badge", badgeCount + 1);

            Map<String, Object> jsonBody = new HashMap<>();
            if (payload.getUrl() != null && !payload.getUrl().isEmpty()) {
                jsonBody.put("external_link", payload.getUrl());
            }
            if (payload.getVideoId() != null && payload.getVideoId() != 0) {
                jsonBody.put("video_id", payload.getVideoId());
            }
            jsonBody.put("notification_id", payload.getNotificationId() != null ? payload.getNotificationId() : "");

            Map<String, Object> apns = new HashMap<>();
            apns.put("aps", aps);
            apns.put("notification_id", payload.getNotificationId());
            apns.put("jsonBody", jsonBody);

            String apnsPayload = GSON.toJson(apns);

            if (environment.equalsIgnoreCase("PROD"))
                message.put("APNS", apnsPayload);

            else if (environment.equalsIgnoreCase("SANDBOX"))
                message.put("APNS_SANDBOX", apnsPayload);

            logger.log(GSON.toJson(message));
        }

        return GSON.toJson(message);
    }

    private void saveNotificationStatus(WorkerPayload payload, LambdaLogger logger) {

        try (Connection conn = DatabaseService.getConnection();
             PreparedStatement mappingSt = conn.prepareStatement(USER_MAPPING_QUERY)) {

            for (String userId : payload.getUserIds()) {
                try {
                    UUID userUuid = UUID.fromString(userId);
                    int notificationId = Integer.parseInt(payload.getNotificationId());

                    mappingSt.setObject(1, userUuid);
                    mappingSt.setInt(2, notificationId);
                    mappingSt.setBoolean(3, false);
                    mappingSt.addBatch();

                } catch (Exception e) {
                    logger.log("ERROR parsing userId/notificationId: " + e.getMessage());
                }
            }

            int[] results = mappingSt.executeBatch();
            logger.log("Database batch completed: " + results.length + " records");

        } catch (Exception e) {
            logger.log("DATABASE ERROR: " + e.getMessage());
            throw new RuntimeException("Failed to update notification status", e);
        }
    }

    private int getUnreadNotificationCount(String userId, LambdaLogger logger) {
        String query = "SELECT COUNT(*) FROM public.user_notification_status WHERE user_id = ? AND read = false";

        try (Connection conn = DatabaseService.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setObject(1, UUID.fromString(userId));
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                int count = rs.getInt(1);
                logger.log("Unread notification count for user " + userId + ": " + count);
                return count;
            }

        } catch (Exception e) {
            logger.log("ERROR fetching unread notification count for user " + userId + ": " + e.getMessage());
            throw new RuntimeException("Failed to fetch unread notification count", e);
        }

        return 0;
    }

    private String buildSilentPlatformMessage(String platform, String environment, int badgeCount, WorkerPayload payload, LambdaLogger logger) {

        Map<String, Object> message = new HashMap<>();
        message.put("default", "");

        if ("ANDROID".equalsIgnoreCase(platform)) {

            Map<String, Object> data = new HashMap<>();
            data.put("badge_count", badgeCount);
            data.put("notification_id", payload.getNotificationId());
            Map<String, Object> gcm = new HashMap<>();
            gcm.put("data", data);

            message.put("GCM", GSON.toJson(gcm));

        } else if ("IOS".equalsIgnoreCase(platform)) {

            Map<String, Object> aps = new HashMap<>();
            aps.put("badge", badgeCount);
            aps.put("content-available", 1);

            Map<String, Object> apns = new HashMap<>();
            apns.put("aps", aps);
            apns.put("notification_id", payload.getNotificationId());

            String apnsPayload = GSON.toJson(apns);

            if (environment.equalsIgnoreCase("PROD"))
                message.put("APNS", apnsPayload);

            else if (environment.equalsIgnoreCase("SANDBOX"))
                message.put("APNS_SANDBOX", apnsPayload);
        }

        return GSON.toJson(message);
    }
}