/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.session.data.cassandra.conversion;

import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.util.Base64Utils;
/**
 * Serialize an {@link java.lang.Object} and convert it to Base64 for storage in Cassandra.
 * @author Andrew Fitzgerald
 * @see StringToObjectDeserializer
 */
public class ObjectToStringSerializer {

	private final SerializingConverter converter = new SerializingConverter();

	public String serializeToString(Object object) {
		byte[] data = this.converter.convert(object);
		return Base64Utils.encodeToString(data);
	}
}
