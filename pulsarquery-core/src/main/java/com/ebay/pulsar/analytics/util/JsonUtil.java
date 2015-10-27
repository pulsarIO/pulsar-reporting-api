/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Utility methods for JSON transformation.
 *
 * @author rtao
 * 
 */
public class JsonUtil {

    // reuse the object mapper to save memory footprint
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectMapper indentMapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        indentMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    public static <T> T readValue(File src, Class<T> valueType) throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(String content, Class<T> valueType) throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(content, valueType);
    }

    public static <T> T readValue(Reader src, Class<T> valueType) throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(InputStream src, Class<T> valueType) throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(byte[] src, Class<T> valueType) throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(src, valueType);
    }

    public static void writeValueIndent(OutputStream out, Object value) throws IOException, JsonGenerationException, JsonMappingException {
        indentMapper.writeValue(out, value);
    }

    public static void writeValue(OutputStream out, Object value) throws IOException, JsonGenerationException, JsonMappingException {
        mapper.writeValue(out, value);
    }

    public static String writeValueAsString(Object value) throws JsonProcessingException {
        return mapper.writeValueAsString(value);
    }

    public static byte[] writeValueAsBytes(Object value) throws JsonProcessingException {
        return mapper.writeValueAsBytes(value);
    }

    public static String writeValueAsIndentString(Object value) throws JsonProcessingException {
        return indentMapper.writeValueAsString(value);
    }

}
