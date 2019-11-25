package org.tugraz.sysds.runtime.transform.encode;

import java.util.HashMap;

import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

public class EncoderFeatureHash extends EncoderRecode {

    private static final long serialVersionUID = 7435806042138687342L;
    private Long K;

    public EncoderFeatureHash(JSONObject parsedSpec, String[] colnames, int clen) throws JSONException {
        super(parsedSpec, colnames, clen);
        this.K = 1000L; //TODO: Make this k configurable by the user. 
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