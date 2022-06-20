package edu.harvard.iq.dataverse.util;

import edu.harvard.iq.dataverse.settings.JvmSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SystemConfigTest {

    SystemConfig systemConfig = new SystemConfig();
    
    @Test
    void testGetVersion() {
        // given
        String version = "100.100";
        System.setProperty(JvmSettings.VERSION.getScopedKey(), version);
        
        // when
        String result = systemConfig.getVersion(false);
        
        // then
        assertEquals(version, result);
    }
    
    @Test
    void testGetVersionWithBuild() {
        // given
        String version = "100.100";
        String build = "FOOBAR";
        System.setProperty(JvmSettings.VERSION.getScopedKey(), version);
        System.setProperty(JvmSettings.BUILD.getScopedKey(), build);
    
        // when
        String result = systemConfig.getVersion(true);
    
        // then
        assertTrue(result.startsWith(version), "'" + result + "' not starting with " + version);
        assertTrue(result.contains("build"));
        
        // Cannot test this here - there might be the bundle file present which is not under test control
        //assertTrue(result.endsWith(build), "'" + result + "' not ending with " + build);
    }
    
    @Test
    void testGetLongLimitFromStringOrDefault_withNullInput() {
        long defaultValue = 5L;
        long actualResult = SystemConfig.getLongLimitFromStringOrDefault(null, defaultValue);
        assertEquals(defaultValue, actualResult);
    }

    @Test
    void testGetIntLimitFromStringOrDefault_withNullInput() {
        int defaultValue = 5;
        int actualResult = SystemConfig.getIntLimitFromStringOrDefault(null, defaultValue);
        assertEquals(defaultValue, actualResult);
    }

    @ParameterizedTest
    @CsvSource({
            ", 5",
            "test, 5",
            "-10, -10",
            "0, 0",
            "10, 10"
    })
    void testGetLongLimitFromStringOrDefault_withStringInputs(String inputString, long expectedResult) {
        long actualResult = SystemConfig.getLongLimitFromStringOrDefault(inputString, 5L);
        assertEquals(expectedResult, actualResult);
    }

    @ParameterizedTest
    @CsvSource({
            ", 5",
            "test, 5",
            "-10, -10",
            "0, 0",
            "10, 10"
    })
    void testGetIntLimitFromStringOrDefault_withStringInputs(String inputString, int expectedResult) {
        int actualResult = SystemConfig.getIntLimitFromStringOrDefault(inputString, 5);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void testGetThumbnailSizeLimit() {
        assertEquals(3000000l, SystemConfig.getThumbnailSizeLimit("Image"));
        assertEquals(1000000l, SystemConfig.getThumbnailSizeLimit("PDF"));
        assertEquals(0l, SystemConfig.getThumbnailSizeLimit("NoSuchType"));
    }

}
