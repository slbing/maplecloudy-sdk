package com.maplecloudy.spider.parse;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.maplecloudy.spider.protocol.Content;

/*
 * Ĭ�ϵ�parse������ʲô������
 */
public class DefaultlParse implements Parse
{
	public static final Log LOG = LogFactory.getLog(DefaultlParse.class);
	
	public List parse(String url, Content content)
	{
		return new ArrayList();
		
	}

	public static void main(String[] args) throws Exception
	{

	}

}
