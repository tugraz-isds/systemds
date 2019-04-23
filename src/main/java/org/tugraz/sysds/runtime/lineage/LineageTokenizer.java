package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.parser.ParseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LineageTokenizer {
	
	private List<TokenInfo> _tokenInfos;
	
	public LineageTokenizer() {
		_tokenInfos = new ArrayList<>();
	}
	
	public void add(String regex, String key) {
		_tokenInfos.add(new TokenInfo(Pattern.compile("^(" + regex + ")"), key));
	}
	
	public void add(String regex) {
		_tokenInfos.add(new TokenInfo(Pattern.compile("^(" + regex + ")"), ""));
	}
	
	
	public Map<String, String> tokenize(String str) {
		Map<String, String> tokens = new HashMap<>();
		
		for (TokenInfo info : _tokenInfos) {
			Matcher m = info.regex.matcher(str);
			if (m.find()) {
				String tok = m.group().trim();
				if (!info.key.isEmpty())
					tokens.put(info.key, tok);
				str = m.replaceFirst("");
			}
		}
		if (!str.isEmpty())
			throw new ParseException("Unexpected character in input: " + str);
		return tokens;
	}
	
	private class TokenInfo {
		public final Pattern regex;
		public final String key;
		
		public TokenInfo(Pattern regex, String key) {
			super();
			this.regex = regex;
			this.key = key;
		}
	}
}