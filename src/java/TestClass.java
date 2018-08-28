package tester;

import java.util.*;

public class TestClass
{
	private Map<String, String> testMap = new HashMap<String, String>();

	public TestClass(String k1, String v1, String k2, String v2)
	{
		testMap.put(k1, v1);
		testMap.put(k2, v2);
	}

	public String getValue(String key)
	{
		
		if(testMap.containsKey(key))
		{
			return testMap.get(key);
		}
		return null;
	}

	public int printThis(Object object)
	{
		System.out.println("THIS OBJECT IS A " + object.getClass().getName());
		System.out.println("CONTENTS: " + object.toString());
		return 0;
	}

	public boolean  isEnabled()
	{
		return true;
	}

	public int cacheTtl()
	{
		return 30;
	}
}
