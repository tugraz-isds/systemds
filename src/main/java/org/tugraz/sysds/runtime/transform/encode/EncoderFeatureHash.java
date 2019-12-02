/*

Copyright 2019 Graz University of Technology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package org.tugraz.sysds.runtime.transform.encode;

import java.util.HashMap;

import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

/**
 * Class used for feature hashing transformation of frames. 
 */
public class EncoderFeatureHash extends EncoderRecode {

    private static final long serialVersionUID = 7435806042138687342L;
    private Long K;

    public EncoderFeatureHash(JSONObject parsedSpec, String[] colnames, int clen) throws JSONException {
		super(parsedSpec, colnames, clen);
        this.K = getK(parsedSpec); 
	}
	
	/**
	 * Get K value used for calculation during feature hashing from parsed specifications.
	 * @param parsedSpec parsed specifications
	 * @return K value
	 * @throws JSONException
	 */
	private Long getK(JSONObject parsedSpec) throws JSONException {
		return parsedSpec.getLong("K");
	}

    /**
	 * Put the code into the map with the provided key. The code depends on the type of encoder. 
	 * @param map column map
	 * @param key key for the new entry
	 */
    @Override
	protected void putCode(HashMap<String,Long> map, String key) {
			map.put(key, (key.hashCode() % this.K));
	}

}