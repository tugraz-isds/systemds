// Generated from org\tugraz\sysds\parser\dml\Dml.g4 by ANTLR 4.5.3
package org.tugraz.sysds.parser.dml;

/*
 * Modifications Copyright 2018 Graz University of Technology
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class DmlLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, T__29=30, T__30=31, 
		T__31=32, T__32=33, T__33=34, T__34=35, T__35=36, T__36=37, T__37=38, 
		T__38=39, T__39=40, T__40=41, T__41=42, T__42=43, T__43=44, T__44=45, 
		T__45=46, T__46=47, T__47=48, T__48=49, T__49=50, T__50=51, T__51=52, 
		T__52=53, T__53=54, T__54=55, T__55=56, T__56=57, T__57=58, T__58=59, 
		ID=60, INT=61, DOUBLE=62, DIGIT=63, ALPHABET=64, COMMANDLINE_NAMED_ID=65, 
		COMMANDLINE_POSITION_ID=66, STRING=67, LINE_COMMENT=68, MULTILINE_BLOCK_COMMENT=69, 
		WHITESPACE=70;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "T__16", 
		"T__17", "T__18", "T__19", "T__20", "T__21", "T__22", "T__23", "T__24", 
		"T__25", "T__26", "T__27", "T__28", "T__29", "T__30", "T__31", "T__32", 
		"T__33", "T__34", "T__35", "T__36", "T__37", "T__38", "T__39", "T__40", 
		"T__41", "T__42", "T__43", "T__44", "T__45", "T__46", "T__47", "T__48", 
		"T__49", "T__50", "T__51", "T__52", "T__53", "T__54", "T__55", "T__56", 
		"T__57", "T__58", "ID", "INT", "DOUBLE", "DIGIT", "ALPHABET", "EXP", "COMMANDLINE_NAMED_ID", 
		"COMMANDLINE_POSITION_ID", "STRING", "ESC", "LINE_COMMENT", "MULTILINE_BLOCK_COMMENT", 
		"WHITESPACE"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'source'", "'('", "')'", "'as'", "';'", "'setwd'", "'='", "'<-'", 
		"','", "'['", "']'", "'ifdef'", "'+='", "'if'", "'{'", "'}'", "'else'", 
		"'for'", "'in'", "'parfor'", "'while'", "':'", "'function'", "'return'", 
		"'externalFunction'", "'implemented'", "'^'", "'-'", "'+'", "'%*%'", "'%/%'", 
		"'%%'", "'*'", "'/'", "'>'", "'>='", "'<'", "'<='", "'=='", "'!='", "'!'", 
		"'&'", "'&&'", "'|'", "'||'", "'TRUE'", "'FALSE'", "'int'", "'integer'", 
		"'string'", "'boolean'", "'double'", "'unknown'", "'Int'", "'Integer'", 
		"'String'", "'Boolean'", "'Double'", "'Unknown'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		"ID", "INT", "DOUBLE", "DIGIT", "ALPHABET", "COMMANDLINE_NAMED_ID", "COMMANDLINE_POSITION_ID", 
		"STRING", "LINE_COMMENT", "MULTILINE_BLOCK_COMMENT", "WHITESPACE"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public DmlLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "Dml.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2H\u02bd\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3"+
		"\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\t\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3"+
		"\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\17\3\17\3\17\3\20\3\20\3\21\3"+
		"\21\3\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\25\3"+
		"\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3"+
		"\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3"+
		"\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3"+
		"\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3"+
		"\33\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3\37\3\37\3 \3 "+
		"\3 \3 \3!\3!\3!\3\"\3\"\3#\3#\3$\3$\3%\3%\3%\3&\3&\3\'\3\'\3\'\3(\3(\3"+
		"(\3)\3)\3)\3*\3*\3+\3+\3,\3,\3,\3-\3-\3.\3.\3.\3/\3/\3/\3/\3/\3\60\3\60"+
		"\3\60\3\60\3\60\3\60\3\61\3\61\3\61\3\61\3\62\3\62\3\62\3\62\3\62\3\62"+
		"\3\62\3\62\3\63\3\63\3\63\3\63\3\63\3\63\3\63\3\64\3\64\3\64\3\64\3\64"+
		"\3\64\3\64\3\64\3\65\3\65\3\65\3\65\3\65\3\65\3\65\3\66\3\66\3\66\3\66"+
		"\3\66\3\66\3\66\3\66\3\67\3\67\3\67\3\67\38\38\38\38\38\38\38\38\39\3"+
		"9\39\39\39\39\39\3:\3:\3:\3:\3:\3:\3:\3:\3;\3;\3;\3;\3;\3;\3;\3<\3<\3"+
		"<\3<\3<\3<\3<\3<\3=\3=\3=\3=\7=\u01a1\n=\f=\16=\u01a4\13=\3=\3=\3=\5="+
		"\u01a9\n=\3=\3=\3=\3=\7=\u01af\n=\f=\16=\u01b2\13=\3=\3=\3=\3=\3=\3=\3"+
		"=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3"+
		"=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3"+
		"=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3"+
		"=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3"+
		"=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3"+
		"=\3=\3=\3=\3=\3=\3=\3=\5=\u0235\n=\3>\6>\u0238\n>\r>\16>\u0239\3>\5>\u023d"+
		"\n>\3?\6?\u0240\n?\r?\16?\u0241\3?\3?\7?\u0246\n?\f?\16?\u0249\13?\3?"+
		"\5?\u024c\n?\3?\5?\u024f\n?\3?\6?\u0252\n?\r?\16?\u0253\3?\5?\u0257\n"+
		"?\3?\5?\u025a\n?\3?\3?\6?\u025e\n?\r?\16?\u025f\3?\5?\u0263\n?\3?\5?\u0266"+
		"\n?\5?\u0268\n?\3@\3@\3A\3A\3B\3B\5B\u0270\nB\3B\3B\3C\3C\3C\3C\3C\7C"+
		"\u0279\nC\fC\16C\u027c\13C\3D\3D\6D\u0280\nD\rD\16D\u0281\3E\3E\3E\7E"+
		"\u0287\nE\fE\16E\u028a\13E\3E\3E\3E\3E\7E\u0290\nE\fE\16E\u0293\13E\3"+
		"E\5E\u0296\nE\3F\3F\3F\3G\3G\7G\u029d\nG\fG\16G\u02a0\13G\3G\5G\u02a3"+
		"\nG\3G\3G\3G\3G\3H\3H\3H\3H\7H\u02ad\nH\fH\16H\u02b0\13H\3H\3H\3H\3H\3"+
		"H\3I\6I\u02b8\nI\rI\16I\u02b9\3I\3I\6\u0288\u0291\u029e\u02ae\2J\3\3\5"+
		"\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21"+
		"!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\36;\37= ?!"+
		"A\"C#E$G%I&K\'M(O)Q*S+U,W-Y.[/]\60_\61a\62c\63e\64g\65i\66k\67m8o9q:s"+
		";u<w=y>{?}@\177A\u0081B\u0083\2\u0085C\u0087D\u0089E\u008b\2\u008dF\u008f"+
		"G\u0091H\3\2\n\4\2NNnn\4\2C\\c|\4\2GGgg\4\2--//\4\2$$^^\4\2))^^\n\2$$"+
		"))^^ddhhppttvv\5\2\13\f\17\17\"\"\u02eb\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3"+
		"\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2"+
		"\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35"+
		"\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)"+
		"\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2"+
		"\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2"+
		"A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3"+
		"\2\2\2\2O\3\2\2\2\2Q\3\2\2\2\2S\3\2\2\2\2U\3\2\2\2\2W\3\2\2\2\2Y\3\2\2"+
		"\2\2[\3\2\2\2\2]\3\2\2\2\2_\3\2\2\2\2a\3\2\2\2\2c\3\2\2\2\2e\3\2\2\2\2"+
		"g\3\2\2\2\2i\3\2\2\2\2k\3\2\2\2\2m\3\2\2\2\2o\3\2\2\2\2q\3\2\2\2\2s\3"+
		"\2\2\2\2u\3\2\2\2\2w\3\2\2\2\2y\3\2\2\2\2{\3\2\2\2\2}\3\2\2\2\2\177\3"+
		"\2\2\2\2\u0081\3\2\2\2\2\u0085\3\2\2\2\2\u0087\3\2\2\2\2\u0089\3\2\2\2"+
		"\2\u008d\3\2\2\2\2\u008f\3\2\2\2\2\u0091\3\2\2\2\3\u0093\3\2\2\2\5\u009a"+
		"\3\2\2\2\7\u009c\3\2\2\2\t\u009e\3\2\2\2\13\u00a1\3\2\2\2\r\u00a3\3\2"+
		"\2\2\17\u00a9\3\2\2\2\21\u00ab\3\2\2\2\23\u00ae\3\2\2\2\25\u00b0\3\2\2"+
		"\2\27\u00b2\3\2\2\2\31\u00b4\3\2\2\2\33\u00ba\3\2\2\2\35\u00bd\3\2\2\2"+
		"\37\u00c0\3\2\2\2!\u00c2\3\2\2\2#\u00c4\3\2\2\2%\u00c9\3\2\2\2\'\u00cd"+
		"\3\2\2\2)\u00d0\3\2\2\2+\u00d7\3\2\2\2-\u00dd\3\2\2\2/\u00df\3\2\2\2\61"+
		"\u00e8\3\2\2\2\63\u00ef\3\2\2\2\65\u0100\3\2\2\2\67\u010c\3\2\2\29\u010e"+
		"\3\2\2\2;\u0110\3\2\2\2=\u0112\3\2\2\2?\u0116\3\2\2\2A\u011a\3\2\2\2C"+
		"\u011d\3\2\2\2E\u011f\3\2\2\2G\u0121\3\2\2\2I\u0123\3\2\2\2K\u0126\3\2"+
		"\2\2M\u0128\3\2\2\2O\u012b\3\2\2\2Q\u012e\3\2\2\2S\u0131\3\2\2\2U\u0133"+
		"\3\2\2\2W\u0135\3\2\2\2Y\u0138\3\2\2\2[\u013a\3\2\2\2]\u013d\3\2\2\2_"+
		"\u0142\3\2\2\2a\u0148\3\2\2\2c\u014c\3\2\2\2e\u0154\3\2\2\2g\u015b\3\2"+
		"\2\2i\u0163\3\2\2\2k\u016a\3\2\2\2m\u0172\3\2\2\2o\u0176\3\2\2\2q\u017e"+
		"\3\2\2\2s\u0185\3\2\2\2u\u018d\3\2\2\2w\u0194\3\2\2\2y\u0234\3\2\2\2{"+
		"\u0237\3\2\2\2}\u0267\3\2\2\2\177\u0269\3\2\2\2\u0081\u026b\3\2\2\2\u0083"+
		"\u026d\3\2\2\2\u0085\u0273\3\2\2\2\u0087\u027d\3\2\2\2\u0089\u0295\3\2"+
		"\2\2\u008b\u0297\3\2\2\2\u008d\u029a\3\2\2\2\u008f\u02a8\3\2\2\2\u0091"+
		"\u02b7\3\2\2\2\u0093\u0094\7u\2\2\u0094\u0095\7q\2\2\u0095\u0096\7w\2"+
		"\2\u0096\u0097\7t\2\2\u0097\u0098\7e\2\2\u0098\u0099\7g\2\2\u0099\4\3"+
		"\2\2\2\u009a\u009b\7*\2\2\u009b\6\3\2\2\2\u009c\u009d\7+\2\2\u009d\b\3"+
		"\2\2\2\u009e\u009f\7c\2\2\u009f\u00a0\7u\2\2\u00a0\n\3\2\2\2\u00a1\u00a2"+
		"\7=\2\2\u00a2\f\3\2\2\2\u00a3\u00a4\7u\2\2\u00a4\u00a5\7g\2\2\u00a5\u00a6"+
		"\7v\2\2\u00a6\u00a7\7y\2\2\u00a7\u00a8\7f\2\2\u00a8\16\3\2\2\2\u00a9\u00aa"+
		"\7?\2\2\u00aa\20\3\2\2\2\u00ab\u00ac\7>\2\2\u00ac\u00ad\7/\2\2\u00ad\22"+
		"\3\2\2\2\u00ae\u00af\7.\2\2\u00af\24\3\2\2\2\u00b0\u00b1\7]\2\2\u00b1"+
		"\26\3\2\2\2\u00b2\u00b3\7_\2\2\u00b3\30\3\2\2\2\u00b4\u00b5\7k\2\2\u00b5"+
		"\u00b6\7h\2\2\u00b6\u00b7\7f\2\2\u00b7\u00b8\7g\2\2\u00b8\u00b9\7h\2\2"+
		"\u00b9\32\3\2\2\2\u00ba\u00bb\7-\2\2\u00bb\u00bc\7?\2\2\u00bc\34\3\2\2"+
		"\2\u00bd\u00be\7k\2\2\u00be\u00bf\7h\2\2\u00bf\36\3\2\2\2\u00c0\u00c1"+
		"\7}\2\2\u00c1 \3\2\2\2\u00c2\u00c3\7\177\2\2\u00c3\"\3\2\2\2\u00c4\u00c5"+
		"\7g\2\2\u00c5\u00c6\7n\2\2\u00c6\u00c7\7u\2\2\u00c7\u00c8\7g\2\2\u00c8"+
		"$\3\2\2\2\u00c9\u00ca\7h\2\2\u00ca\u00cb\7q\2\2\u00cb\u00cc\7t\2\2\u00cc"+
		"&\3\2\2\2\u00cd\u00ce\7k\2\2\u00ce\u00cf\7p\2\2\u00cf(\3\2\2\2\u00d0\u00d1"+
		"\7r\2\2\u00d1\u00d2\7c\2\2\u00d2\u00d3\7t\2\2\u00d3\u00d4\7h\2\2\u00d4"+
		"\u00d5\7q\2\2\u00d5\u00d6\7t\2\2\u00d6*\3\2\2\2\u00d7\u00d8\7y\2\2\u00d8"+
		"\u00d9\7j\2\2\u00d9\u00da\7k\2\2\u00da\u00db\7n\2\2\u00db\u00dc\7g\2\2"+
		"\u00dc,\3\2\2\2\u00dd\u00de\7<\2\2\u00de.\3\2\2\2\u00df\u00e0\7h\2\2\u00e0"+
		"\u00e1\7w\2\2\u00e1\u00e2\7p\2\2\u00e2\u00e3\7e\2\2\u00e3\u00e4\7v\2\2"+
		"\u00e4\u00e5\7k\2\2\u00e5\u00e6\7q\2\2\u00e6\u00e7\7p\2\2\u00e7\60\3\2"+
		"\2\2\u00e8\u00e9\7t\2\2\u00e9\u00ea\7g\2\2\u00ea\u00eb\7v\2\2\u00eb\u00ec"+
		"\7w\2\2\u00ec\u00ed\7t\2\2\u00ed\u00ee\7p\2\2\u00ee\62\3\2\2\2\u00ef\u00f0"+
		"\7g\2\2\u00f0\u00f1\7z\2\2\u00f1\u00f2\7v\2\2\u00f2\u00f3\7g\2\2\u00f3"+
		"\u00f4\7t\2\2\u00f4\u00f5\7p\2\2\u00f5\u00f6\7c\2\2\u00f6\u00f7\7n\2\2"+
		"\u00f7\u00f8\7H\2\2\u00f8\u00f9\7w\2\2\u00f9\u00fa\7p\2\2\u00fa\u00fb"+
		"\7e\2\2\u00fb\u00fc\7v\2\2\u00fc\u00fd\7k\2\2\u00fd\u00fe\7q\2\2\u00fe"+
		"\u00ff\7p\2\2\u00ff\64\3\2\2\2\u0100\u0101\7k\2\2\u0101\u0102\7o\2\2\u0102"+
		"\u0103\7r\2\2\u0103\u0104\7n\2\2\u0104\u0105\7g\2\2\u0105\u0106\7o\2\2"+
		"\u0106\u0107\7g\2\2\u0107\u0108\7p\2\2\u0108\u0109\7v\2\2\u0109\u010a"+
		"\7g\2\2\u010a\u010b\7f\2\2\u010b\66\3\2\2\2\u010c\u010d\7`\2\2\u010d8"+
		"\3\2\2\2\u010e\u010f\7/\2\2\u010f:\3\2\2\2\u0110\u0111\7-\2\2\u0111<\3"+
		"\2\2\2\u0112\u0113\7\'\2\2\u0113\u0114\7,\2\2\u0114\u0115\7\'\2\2\u0115"+
		">\3\2\2\2\u0116\u0117\7\'\2\2\u0117\u0118\7\61\2\2\u0118\u0119\7\'\2\2"+
		"\u0119@\3\2\2\2\u011a\u011b\7\'\2\2\u011b\u011c\7\'\2\2\u011cB\3\2\2\2"+
		"\u011d\u011e\7,\2\2\u011eD\3\2\2\2\u011f\u0120\7\61\2\2\u0120F\3\2\2\2"+
		"\u0121\u0122\7@\2\2\u0122H\3\2\2\2\u0123\u0124\7@\2\2\u0124\u0125\7?\2"+
		"\2\u0125J\3\2\2\2\u0126\u0127\7>\2\2\u0127L\3\2\2\2\u0128\u0129\7>\2\2"+
		"\u0129\u012a\7?\2\2\u012aN\3\2\2\2\u012b\u012c\7?\2\2\u012c\u012d\7?\2"+
		"\2\u012dP\3\2\2\2\u012e\u012f\7#\2\2\u012f\u0130\7?\2\2\u0130R\3\2\2\2"+
		"\u0131\u0132\7#\2\2\u0132T\3\2\2\2\u0133\u0134\7(\2\2\u0134V\3\2\2\2\u0135"+
		"\u0136\7(\2\2\u0136\u0137\7(\2\2\u0137X\3\2\2\2\u0138\u0139\7~\2\2\u0139"+
		"Z\3\2\2\2\u013a\u013b\7~\2\2\u013b\u013c\7~\2\2\u013c\\\3\2\2\2\u013d"+
		"\u013e\7V\2\2\u013e\u013f\7T\2\2\u013f\u0140\7W\2\2\u0140\u0141\7G\2\2"+
		"\u0141^\3\2\2\2\u0142\u0143\7H\2\2\u0143\u0144\7C\2\2\u0144\u0145\7N\2"+
		"\2\u0145\u0146\7U\2\2\u0146\u0147\7G\2\2\u0147`\3\2\2\2\u0148\u0149\7"+
		"k\2\2\u0149\u014a\7p\2\2\u014a\u014b\7v\2\2\u014bb\3\2\2\2\u014c\u014d"+
		"\7k\2\2\u014d\u014e\7p\2\2\u014e\u014f\7v\2\2\u014f\u0150\7g\2\2\u0150"+
		"\u0151\7i\2\2\u0151\u0152\7g\2\2\u0152\u0153\7t\2\2\u0153d\3\2\2\2\u0154"+
		"\u0155\7u\2\2\u0155\u0156\7v\2\2\u0156\u0157\7t\2\2\u0157\u0158\7k\2\2"+
		"\u0158\u0159\7p\2\2\u0159\u015a\7i\2\2\u015af\3\2\2\2\u015b\u015c\7d\2"+
		"\2\u015c\u015d\7q\2\2\u015d\u015e\7q\2\2\u015e\u015f\7n\2\2\u015f\u0160"+
		"\7g\2\2\u0160\u0161\7c\2\2\u0161\u0162\7p\2\2\u0162h\3\2\2\2\u0163\u0164"+
		"\7f\2\2\u0164\u0165\7q\2\2\u0165\u0166\7w\2\2\u0166\u0167\7d\2\2\u0167"+
		"\u0168\7n\2\2\u0168\u0169\7g\2\2\u0169j\3\2\2\2\u016a\u016b\7w\2\2\u016b"+
		"\u016c\7p\2\2\u016c\u016d\7m\2\2\u016d\u016e\7p\2\2\u016e\u016f\7q\2\2"+
		"\u016f\u0170\7y\2\2\u0170\u0171\7p\2\2\u0171l\3\2\2\2\u0172\u0173\7K\2"+
		"\2\u0173\u0174\7p\2\2\u0174\u0175\7v\2\2\u0175n\3\2\2\2\u0176\u0177\7"+
		"K\2\2\u0177\u0178\7p\2\2\u0178\u0179\7v\2\2\u0179\u017a\7g\2\2\u017a\u017b"+
		"\7i\2\2\u017b\u017c\7g\2\2\u017c\u017d\7t\2\2\u017dp\3\2\2\2\u017e\u017f"+
		"\7U\2\2\u017f\u0180\7v\2\2\u0180\u0181\7t\2\2\u0181\u0182\7k\2\2\u0182"+
		"\u0183\7p\2\2\u0183\u0184\7i\2\2\u0184r\3\2\2\2\u0185\u0186\7D\2\2\u0186"+
		"\u0187\7q\2\2\u0187\u0188\7q\2\2\u0188\u0189\7n\2\2\u0189\u018a\7g\2\2"+
		"\u018a\u018b\7c\2\2\u018b\u018c\7p\2\2\u018ct\3\2\2\2\u018d\u018e\7F\2"+
		"\2\u018e\u018f\7q\2\2\u018f\u0190\7w\2\2\u0190\u0191\7d\2\2\u0191\u0192"+
		"\7n\2\2\u0192\u0193\7g\2\2\u0193v\3\2\2\2\u0194\u0195\7W\2\2\u0195\u0196"+
		"\7p\2\2\u0196\u0197\7m\2\2\u0197\u0198\7p\2\2\u0198\u0199\7q\2\2\u0199"+
		"\u019a\7y\2\2\u019a\u019b\7p\2\2\u019bx\3\2\2\2\u019c\u01a2\5\u0081A\2"+
		"\u019d\u01a1\5\u0081A\2\u019e\u01a1\5\177@\2\u019f\u01a1\7a\2\2\u01a0"+
		"\u019d\3\2\2\2\u01a0\u019e\3\2\2\2\u01a0\u019f\3\2\2\2\u01a1\u01a4\3\2"+
		"\2\2\u01a2\u01a0\3\2\2\2\u01a2\u01a3\3\2\2\2\u01a3\u01a5\3\2\2\2\u01a4"+
		"\u01a2\3\2\2\2\u01a5\u01a6\7<\2\2\u01a6\u01a7\7<\2\2\u01a7\u01a9\3\2\2"+
		"\2\u01a8\u019c\3\2\2\2\u01a8\u01a9\3\2\2\2\u01a9\u01aa\3\2\2\2\u01aa\u01b0"+
		"\5\u0081A\2\u01ab\u01af\5\u0081A\2\u01ac\u01af\5\177@\2\u01ad\u01af\7"+
		"a\2\2\u01ae\u01ab\3\2\2\2\u01ae\u01ac\3\2\2\2\u01ae\u01ad\3\2\2\2\u01af"+
		"\u01b2\3\2\2\2\u01b0\u01ae\3\2\2\2\u01b0\u01b1\3\2\2\2\u01b1\u0235\3\2"+
		"\2\2\u01b2\u01b0\3\2\2\2\u01b3\u01b4\7c\2\2\u01b4\u01b5\7u\2\2\u01b5\u01b6"+
		"\7\60\2\2\u01b6\u01b7\7u\2\2\u01b7\u01b8\7e\2\2\u01b8\u01b9\7c\2\2\u01b9"+
		"\u01ba\7n\2\2\u01ba\u01bb\7c\2\2\u01bb\u0235\7t\2\2\u01bc\u01bd\7c\2\2"+
		"\u01bd\u01be\7u\2\2\u01be\u01bf\7\60\2\2\u01bf\u01c0\7o\2\2\u01c0\u01c1"+
		"\7c\2\2\u01c1\u01c2\7v\2\2\u01c2\u01c3\7t\2\2\u01c3\u01c4\7k\2\2\u01c4"+
		"\u0235\7z\2\2\u01c5\u01c6\7c\2\2\u01c6\u01c7\7u\2\2\u01c7\u01c8\7\60\2"+
		"\2\u01c8\u01c9\7h\2\2\u01c9\u01ca\7t\2\2\u01ca\u01cb\7c\2\2\u01cb\u01cc"+
		"\7o\2\2\u01cc\u0235\7g\2\2\u01cd\u01ce\7c\2\2\u01ce\u01cf\7u\2\2\u01cf"+
		"\u01d0\7\60\2\2\u01d0\u01d1\7f\2\2\u01d1\u01d2\7q\2\2\u01d2\u01d3\7w\2"+
		"\2\u01d3\u01d4\7d\2\2\u01d4\u01d5\7n\2\2\u01d5\u0235\7g\2\2\u01d6\u01d7"+
		"\7c\2\2\u01d7\u01d8\7u\2\2\u01d8\u01d9\7\60\2\2\u01d9\u01da\7k\2\2\u01da"+
		"\u01db\7p\2\2\u01db\u01dc\7v\2\2\u01dc\u01dd\7g\2\2\u01dd\u01de\7i\2\2"+
		"\u01de\u01df\7g\2\2\u01df\u0235\7t\2\2\u01e0\u01e1\7c\2\2\u01e1\u01e2"+
		"\7u\2\2\u01e2\u01e3\7\60\2\2\u01e3\u01e4\7n\2\2\u01e4\u01e5\7q\2\2\u01e5"+
		"\u01e6\7i\2\2\u01e6\u01e7\7k\2\2\u01e7\u01e8\7e\2\2\u01e8\u01e9\7c\2\2"+
		"\u01e9\u0235\7n\2\2\u01ea\u01eb\7k\2\2\u01eb\u01ec\7p\2\2\u01ec\u01ed"+
		"\7f\2\2\u01ed\u01ee\7g\2\2\u01ee\u01ef\7z\2\2\u01ef\u01f0\7\60\2\2\u01f0"+
		"\u01f1\7t\2\2\u01f1\u01f2\7g\2\2\u01f2\u01f3\7v\2\2\u01f3\u01f4\7w\2\2"+
		"\u01f4\u01f5\7t\2\2\u01f5\u0235\7p\2\2\u01f6\u01f7\7g\2\2\u01f7\u01f8"+
		"\7o\2\2\u01f8\u01f9\7r\2\2\u01f9\u01fa\7v\2\2\u01fa\u01fb\7{\2\2\u01fb"+
		"\u01fc\7\60\2\2\u01fc\u01fd\7t\2\2\u01fd\u01fe\7g\2\2\u01fe\u01ff\7v\2"+
		"\2\u01ff\u0200\7w\2\2\u0200\u0201\7t\2\2\u0201\u0235\7p\2\2\u0202\u0203"+
		"\7n\2\2\u0203\u0204\7q\2\2\u0204\u0205\7y\2\2\u0205\u0206\7g\2\2\u0206"+
		"\u0207\7t\2\2\u0207\u0208\7\60\2\2\u0208\u0209\7v\2\2\u0209\u020a\7c\2"+
		"\2\u020a\u020b\7k\2\2\u020b\u0235\7n\2\2\u020c\u020d\7n\2\2\u020d\u020e"+
		"\7q\2\2\u020e\u020f\7y\2\2\u020f\u0210\7g\2\2\u0210\u0211\7t\2\2\u0211"+
		"\u0212\7\60\2\2\u0212\u0213\7v\2\2\u0213\u0214\7t\2\2\u0214\u0235\7k\2"+
		"\2\u0215\u0216\7w\2\2\u0216\u0217\7r\2\2\u0217\u0218\7r\2\2\u0218\u0219"+
		"\7g\2\2\u0219\u021a\7t\2\2\u021a\u021b\7\60\2\2\u021b\u021c\7v\2\2\u021c"+
		"\u021d\7t\2\2\u021d\u0235\7k\2\2\u021e\u021f\7k\2\2\u021f\u0220\7u\2\2"+
		"\u0220\u0221\7\60\2\2\u0221\u0222\7p\2\2\u0222\u0235\7c\2\2\u0223\u0224"+
		"\7k\2\2\u0224\u0225\7u\2\2\u0225\u0226\7\60\2\2\u0226\u0227\7p\2\2\u0227"+
		"\u0228\7c\2\2\u0228\u0235\7p\2\2\u0229\u022a\7k\2\2\u022a\u022b\7u\2\2"+
		"\u022b\u022c\7\60\2\2\u022c\u022d\7k\2\2\u022d\u022e\7p\2\2\u022e\u022f"+
		"\7h\2\2\u022f\u0230\7k\2\2\u0230\u0231\7p\2\2\u0231\u0232\7k\2\2\u0232"+
		"\u0233\7v\2\2\u0233\u0235\7g\2\2\u0234\u01a8\3\2\2\2\u0234\u01b3\3\2\2"+
		"\2\u0234\u01bc\3\2\2\2\u0234\u01c5\3\2\2\2\u0234\u01cd\3\2\2\2\u0234\u01d6"+
		"\3\2\2\2\u0234\u01e0\3\2\2\2\u0234\u01ea\3\2\2\2\u0234\u01f6\3\2\2\2\u0234"+
		"\u0202\3\2\2\2\u0234\u020c\3\2\2\2\u0234\u0215\3\2\2\2\u0234\u021e\3\2"+
		"\2\2\u0234\u0223\3\2\2\2\u0234\u0229\3\2\2\2\u0235z\3\2\2\2\u0236\u0238"+
		"\5\177@\2\u0237\u0236\3\2\2\2\u0238\u0239\3\2\2\2\u0239\u0237\3\2\2\2"+
		"\u0239\u023a\3\2\2\2\u023a\u023c\3\2\2\2\u023b\u023d\t\2\2\2\u023c\u023b"+
		"\3\2\2\2\u023c\u023d\3\2\2\2\u023d|\3\2\2\2\u023e\u0240\5\177@\2\u023f"+
		"\u023e\3\2\2\2\u0240\u0241\3\2\2\2\u0241\u023f\3\2\2\2\u0241\u0242\3\2"+
		"\2\2\u0242\u0243\3\2\2\2\u0243\u0247\7\60\2\2\u0244\u0246\5\177@\2\u0245"+
		"\u0244\3\2\2\2\u0246\u0249\3\2\2\2\u0247\u0245\3\2\2\2\u0247\u0248\3\2"+
		"\2\2\u0248\u024b\3\2\2\2\u0249\u0247\3\2\2\2\u024a\u024c\5\u0083B\2\u024b"+
		"\u024a\3\2\2\2\u024b\u024c\3\2\2\2\u024c\u024e\3\2\2\2\u024d\u024f\t\2"+
		"\2\2\u024e\u024d\3\2\2\2\u024e\u024f\3\2\2\2\u024f\u0268\3\2\2\2\u0250"+
		"\u0252\5\177@\2\u0251\u0250\3\2\2\2\u0252\u0253\3\2\2\2\u0253\u0251\3"+
		"\2\2\2\u0253\u0254\3\2\2\2\u0254\u0256\3\2\2\2\u0255\u0257\5\u0083B\2"+
		"\u0256\u0255\3\2\2\2\u0256\u0257\3\2\2\2\u0257\u0259\3\2\2\2\u0258\u025a"+
		"\t\2\2\2\u0259\u0258\3\2\2\2\u0259\u025a\3\2\2\2\u025a\u0268\3\2\2\2\u025b"+
		"\u025d\7\60\2\2\u025c\u025e\5\177@\2\u025d\u025c\3\2\2\2\u025e\u025f\3"+
		"\2\2\2\u025f\u025d\3\2\2\2\u025f\u0260\3\2\2\2\u0260\u0262\3\2\2\2\u0261"+
		"\u0263\5\u0083B\2\u0262\u0261\3\2\2\2\u0262\u0263\3\2\2\2\u0263\u0265"+
		"\3\2\2\2\u0264\u0266\t\2\2\2\u0265\u0264\3\2\2\2\u0265\u0266\3\2\2\2\u0266"+
		"\u0268\3\2\2\2\u0267\u023f\3\2\2\2\u0267\u0251\3\2\2\2\u0267\u025b\3\2"+
		"\2\2\u0268~\3\2\2\2\u0269\u026a\4\62;\2\u026a\u0080\3\2\2\2\u026b\u026c"+
		"\t\3\2\2\u026c\u0082\3\2\2\2\u026d\u026f\t\4\2\2\u026e\u0270\t\5\2\2\u026f"+
		"\u026e\3\2\2\2\u026f\u0270\3\2\2\2\u0270\u0271\3\2\2\2\u0271\u0272\5{"+
		">\2\u0272\u0084\3\2\2\2\u0273\u0274\7&\2\2\u0274\u027a\5\u0081A\2\u0275"+
		"\u0279\5\u0081A\2\u0276\u0279\5\177@\2\u0277\u0279\7a\2\2\u0278\u0275"+
		"\3\2\2\2\u0278\u0276\3\2\2\2\u0278\u0277\3\2\2\2\u0279\u027c\3\2\2\2\u027a"+
		"\u0278\3\2\2\2\u027a\u027b\3\2\2\2\u027b\u0086\3\2\2\2\u027c\u027a\3\2"+
		"\2\2\u027d\u027f\7&\2\2\u027e\u0280\5\177@\2\u027f\u027e\3\2\2\2\u0280"+
		"\u0281\3\2\2\2\u0281\u027f\3\2\2\2\u0281\u0282\3\2\2\2\u0282\u0088\3\2"+
		"\2\2\u0283\u0288\7$\2\2\u0284\u0287\5\u008bF\2\u0285\u0287\n\6\2\2\u0286"+
		"\u0284\3\2\2\2\u0286\u0285\3\2\2\2\u0287\u028a\3\2\2\2\u0288\u0289\3\2"+
		"\2\2\u0288\u0286\3\2\2\2\u0289\u028b\3\2\2\2\u028a\u0288\3\2\2\2\u028b"+
		"\u0296\7$\2\2\u028c\u0291\7)\2\2\u028d\u0290\5\u008bF\2\u028e\u0290\n"+
		"\7\2\2\u028f\u028d\3\2\2\2\u028f\u028e\3\2\2\2\u0290\u0293\3\2\2\2\u0291"+
		"\u0292\3\2\2\2\u0291\u028f\3\2\2\2\u0292\u0294\3\2\2\2\u0293\u0291\3\2"+
		"\2\2\u0294\u0296\7)\2\2\u0295\u0283\3\2\2\2\u0295\u028c\3\2\2\2\u0296"+
		"\u008a\3\2\2\2\u0297\u0298\7^\2\2\u0298\u0299\t\b\2\2\u0299\u008c\3\2"+
		"\2\2\u029a\u029e\7%\2\2\u029b\u029d\13\2\2\2\u029c\u029b\3\2\2\2\u029d"+
		"\u02a0\3\2\2\2\u029e\u029f\3\2\2\2\u029e\u029c\3\2\2\2\u029f\u02a2\3\2"+
		"\2\2\u02a0\u029e\3\2\2\2\u02a1\u02a3\7\17\2\2\u02a2\u02a1\3\2\2\2\u02a2"+
		"\u02a3\3\2\2\2\u02a3\u02a4\3\2\2\2\u02a4\u02a5\7\f\2\2\u02a5\u02a6\3\2"+
		"\2\2\u02a6\u02a7\bG\2\2\u02a7\u008e\3\2\2\2\u02a8\u02a9\7\61\2\2\u02a9"+
		"\u02aa\7,\2\2\u02aa\u02ae\3\2\2\2\u02ab\u02ad\13\2\2\2\u02ac\u02ab\3\2"+
		"\2\2\u02ad\u02b0\3\2\2\2\u02ae\u02af\3\2\2\2\u02ae\u02ac\3\2\2\2\u02af"+
		"\u02b1\3\2\2\2\u02b0\u02ae\3\2\2\2\u02b1\u02b2\7,\2\2\u02b2\u02b3\7\61"+
		"\2\2\u02b3\u02b4\3\2\2\2\u02b4\u02b5\bH\2\2\u02b5\u0090\3\2\2\2\u02b6"+
		"\u02b8\t\t\2\2\u02b7\u02b6\3\2\2\2\u02b8\u02b9\3\2\2\2\u02b9\u02b7\3\2"+
		"\2\2\u02b9\u02ba\3\2\2\2\u02ba\u02bb\3\2\2\2\u02bb\u02bc\bI\2\2\u02bc"+
		"\u0092\3\2\2\2#\2\u01a0\u01a2\u01a8\u01ae\u01b0\u0234\u0239\u023c\u0241"+
		"\u0247\u024b\u024e\u0253\u0256\u0259\u025f\u0262\u0265\u0267\u026f\u0278"+
		"\u027a\u0281\u0286\u0288\u028f\u0291\u0295\u029e\u02a2\u02ae\u02b9\3\b"+
		"\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}