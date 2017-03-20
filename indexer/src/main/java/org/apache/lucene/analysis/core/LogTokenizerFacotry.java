package org.apache.lucene.analysis.core;

import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.AttributeSource.AttributeFactory;



public class LogTokenizerFacotry extends  StandardTokenizerFactory implements ResourceLoaderAware{

	@Override
	public void inform(ResourceLoader paramResourceLoader) throws IOException {
		
	}
	


}