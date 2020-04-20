/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dobest.kafka.serialization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * JSON deserializer for Jackson's JsonNode tree model. Using the tree model allows it to work with arbitrarily
 * structured data without having associated Java classes. This deserializer also supports Connect schemas.
 */
public class JsonDeserializer implements Deserializer<Object> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Class<?> clazz;

    public static final String VALUE_DESERIALIZER_JSON_CLASS_CONFIG = "value.deserializer.json.class";


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);

        Object className = configs.get(VALUE_DESERIALIZER_JSON_CLASS_CONFIG);
        if (className == null)
            throw new SerializationException(VALUE_DESERIALIZER_JSON_CLASS_CONFIG + " is null");
        if (className instanceof String) {
            try {
                clazz = Class.forName((String) className);
            } catch (ClassNotFoundException e) {
                throw new SerializationException("can't find class for name " + className, e);
            }
        } else if (className instanceof Class) {
            clazz = (Class) className;
        }
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;
        Object data;
        StringBuffer sb = new StringBuffer();
        try {
            data = objectMapper.readValue(bytes, clazz);
        } catch (Exception e) {
            throw new SerializationException(e);
        }


        return data;
    }
}
