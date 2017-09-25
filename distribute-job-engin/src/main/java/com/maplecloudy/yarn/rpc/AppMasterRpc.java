package com.maplecloudy.yarn.rpc;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.maplecloudy.distribute.engine.utils.YarnCompat;


class AppMasterRpc implements AutoCloseable {

    private final YarnConfiguration cfg;
    private AMRMClient<ContainerRequest> client;
    private final NMTokenCache nmTokenCache;

    public AppMasterRpc(Configuration cfg, NMTokenCache nmTokenCache) {
        this.cfg = new YarnConfiguration(cfg);
        this.nmTokenCache = nmTokenCache;
    }

    public void start() {
        if (client != null) {
            return;
        }

        client = AMRMClient.createAMRMClient();
        YarnCompat.setNMTokenCache(client, nmTokenCache);
        client.init(cfg);
        client.start();
    }

    public RegisterApplicationMasterResponse registerAM() throws YarnException, IOException {
            return client.registerApplicationMaster("", 0, "");
    }

    public void failAM() throws YarnException, IOException {
        unregisterAM(FinalApplicationStatus.FAILED);
    }

    public void finishAM() throws YarnException, IOException {
        unregisterAM(FinalApplicationStatus.SUCCEEDED);
    }

    private void unregisterAM(FinalApplicationStatus status) throws YarnException, IOException {
            client.unregisterApplicationMaster(status, "", "");
    }

    public void addContainerRequest(ContainerRequest req) {
        client.addContainerRequest(req);
    }

    public AllocateResponse allocate(int step) throws YarnException, IOException {
            return client.allocate(step);
    }

    public Configuration getConfiguration() {
        return cfg;
    }

    public NMTokenCache getNMToCache() {
        return nmTokenCache;
    }

    public void close() throws Exception {
        if (client == null) {
            return;
        }

        client.stop();
        client = null;
    }
 }
