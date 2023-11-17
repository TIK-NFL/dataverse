package edu.harvard.iq.dataverse.dataaccess;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;

public interface GlobusAccessibleStore {

    static final String MANAGED = "managed";
    static final String TRANSFER_ENDPOINT_WITH_BASEPATH = "transfer-endpoint-with-basepath";
    static final String GLOBUS_TOKEN = "globus-token";
    
    public static boolean isDataverseManaged(String driverId) {
        return Boolean.parseBoolean(StorageIO.getConfigParamForDriver(driverId, MANAGED));
    }
    
    public static String getTransferEndpointId(String driverId) {
        String endpointWithBasePath = StorageIO.getConfigParamForDriver(driverId, TRANSFER_ENDPOINT_WITH_BASEPATH);
        int pathStart = endpointWithBasePath.indexOf("/");
        return pathStart > 0 ? endpointWithBasePath.substring(0, pathStart) : endpointWithBasePath;
    }
    
    public static String getTransferPath(String driverId) {
        String endpointWithBasePath = StorageIO.getConfigParamForDriver(driverId, TRANSFER_ENDPOINT_WITH_BASEPATH);
        int pathStart = endpointWithBasePath.indexOf("/");
        return pathStart > 0 ? endpointWithBasePath.substring(pathStart) : "";

    }

    public static JsonArray getReferenceEndpointsWithPaths(String driverId) {
        String[] endpoints = StorageIO.getConfigParamForDriver(driverId, AbstractRemoteOverlayAccessIO.REFERENCE_ENDPOINTS_WITH_BASEPATHS).split("\\s*,\\s*");
        JsonArrayBuilder builder = Json.createArrayBuilder();
        for(int i=0;i<endpoints.length;i++) {
            builder.add(endpoints[i]);
        }
        return builder.build();
    }
    
    public static boolean acceptsGlobusTransfers(String storeId) {
        if(StorageIO.getConfigParamForDriver(storeId, TRANSFER_ENDPOINT_WITH_BASEPATH) != null) {
            return true;
        }
        return false;
    }

    public static boolean allowsGlobusReferences(String storeId) {
        if(StorageIO.getConfigParamForDriver(storeId, AbstractRemoteOverlayAccessIO.REFERENCE_ENDPOINTS_WITH_BASEPATHS) != null) {
            return true;
        }
        return false;
    }
    
    public static String getGlobusToken(String storeId) {
        return StorageIO.getConfigParamForDriver(storeId, GLOBUS_TOKEN);
    }
    
}
