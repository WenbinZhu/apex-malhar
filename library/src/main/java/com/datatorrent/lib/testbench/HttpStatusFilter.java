/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.testbench;


import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

/**
 */
public class HttpStatusFilter extends BaseOperator
{
	private String filterStatus;
	private Map<String, Integer> collect;
	public final transient DefaultInputPort<Map<String, String>> inport = new DefaultInputPort<Map<String, String>>() {
    @Override
    public void process(Map<String, String> s) {
    	for(Map.Entry<String, String> entry : s.entrySet())
    	{
    		if (!entry.getValue().equals(filterStatus)) continue;
	    	if (collect.containsKey(entry.getKey()))
	    	{
	    		Integer value = (Integer)collect.remove(entry.getKey());
	    		collect.put(entry.getKey(), new Integer(value+1));
	    	} else {
	    		collect.put(entry.getKey(), new Integer(1));
	    	}
    	}
    }
	};

	@Override
	public void setup(OperatorContext context)
	{
		collect  = new HashMap<String, Integer>();
	}

	@Override
	public void teardown()
	{
	}

	@Override
	public void beginWindow(long windowId)
	{
		collect  = new HashMap<String, Integer>();
	}
	
	// out port
	public final transient DefaultOutputPort<Map<String, Integer>> outport = new DefaultOutputPort<Map<String, Integer>>();
	
	@Override
	public void endWindow()
	{
		outport.emit(collect);
	}

	public String getFilterStatus()
	{
		return filterStatus;
	}

	public void setFilterStatus(String filterStatus)
	{
		this.filterStatus = filterStatus;
	}
}
