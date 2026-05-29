package com.foobar.api.baz.cofigurations;

import lombok.Builder;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

@Builder
public class SecretManagerConfigurations {

    SecretsManagerClient client;
    private String awsRegion;
    private String profileName;
    private String secretId;

    public String getSecretValue() {

        Region region = Region.of(awsRegion);
        ProfileCredentialsProvider profile =
                ( profileName == null || profileName.isEmpty() ) ?
                        null
                        :
                        ProfileCredentialsProvider.builder().profileName( profileName ).build();

        client = SecretsManagerClient.builder()
                .region(region)
                .credentialsProvider(profile)
                .build();

        GetSecretValueRequest req = GetSecretValueRequest.builder()
                .secretId( secretId )
                .build();

        GetSecretValueResponse res = client.getSecretValue( req );

        return res.secretString();
    }

}
